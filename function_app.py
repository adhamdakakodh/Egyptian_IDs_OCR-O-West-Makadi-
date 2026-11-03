import math
import os
import io
import json
import logging
import re
import statistics
import time
from contextlib import contextmanager
import pyodbc
import requests
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

try:
    AZURE_DOC_INTEL_ENDPOINT = os.environ["AZURE_DOC_INTEL_ENDPOINT"].rstrip("/")
    AZURE_DOC_INTEL_KEY = os.environ["AZURE_DOC_INTEL_KEY"]
    sql_connect_str = os.environ["SqlConnectionString"]
except KeyError as e:
    raise Exception(f"ERROR: Missing Application Setting {e}") from e

DI_API_VERSION = "2024-11-30"
DI_MODEL_ID = os.getenv("DI_MODEL_ID", "prebuilt-read")
DI_FEATURES = os.getenv("DI_FEATURES", "").strip()
DI_POLL_INTERVAL = int(os.getenv("DI_POLL_INTERVAL_MS", "400")) / 1000
DI_TIMEOUT_S = 30.0
IMAGE_MAX_SIDE_PX = int(os.getenv("IMAGE_MAX_SIDE_PX") or 0)
LEVEL_MIN_DEGREES = float(os.getenv("LEVEL_MIN_DEGREES", "1.5"))
SLOT_MATCH = 0.4
FIRST_NAME_SLOT_MATCH = 0.55
LAST_NAME_SLOT_MATCH = 0.55

di_session = requests.Session()
di_session.headers.update({"Ocp-Apim-Subscription-Key": AZURE_DOC_INTEL_KEY})

ARABIC_TO_ENGLISH_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_DIACRITICS = re.compile(r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06ED\u0640]")
_DATE_ONLY = re.compile(r"^\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4}$")
_CARD_SERIAL = re.compile(r"^[a-z]{1,4}\d{5,12}$")

# Card chrome. A line made only of these words is not a data field.
_HEADER_WORDS = {
    "جمهوريه", "مصر", "العربيه", "وزاره", "الداخليه",
    "بطاقه", "تحقيق", "شخصيه", "الرقم", "القومي",
    "arab", "republic", "of", "egypt", "ministry", "interior", "card",
}

# Whole-line labels that sometimes appear beside the four data lines.
_STANDALONE_LABELS = {
    "ذكر", "انثي", "مسلم", "مسلمه", "مسيحي", "مسيحيه", "مصري", "مصريه",
}

# Longer phrases first so "شمال سيناء" wins over a shorter fragment.
_GOVERNORATE_NAMES = [
    "القاهرة", "الجيزة", "الإسكندرية", "الدقهلية", "الشرقية", "القليوبية",
    "كفر الشيخ", "الغربية", "المنوفية", "البحيرة", "الإسماعيلية", "بورسعيد",
    "بور سعيد", "السويس", "شمال سيناء", "جنوب سيناء", "دمياط", "الفيوم",
    "بني سويف", "المنيا", "أسيوط", "سوهاج", "قنا", "الأقصر", "أسوان",
    "البحر الأحمر", "الوادي الجديد", "مطروح", "مرسى مطروح", "حلوان",
    "السادس من أكتوبر", "6 أكتوبر",
]


def fold_arabic(text: str) -> str:
    """Normalize hamza, alef maqsura, and ta marbuta so matching stays stable."""
    folded = (text or "").translate(ARABIC_TO_ENGLISH_DIGITS).lower()
    for src, dst in (
        ("أ", "ا"), ("إ", "ا"), ("آ", "ا"), ("ى", "ي"), ("ة", "ه"),
        ("ؤ", "و"), ("ئ", "ي"),
    ):
        folded = folded.replace(src, dst)
    folded = _DIACRITICS.sub("", folded)
    folded = re.sub(r"[^\w\s]", " ", folded, flags=re.UNICODE)
    return re.sub(r"\s+", " ", folded).strip()


def _folded_set(words: set[str]) -> set[str]:
    return {fold_arabic(word) for word in words}


HEADER_WORDS = _folded_set(_HEADER_WORDS)
STANDALONE_LABELS = _folded_set(_STANDALONE_LABELS)
_HEADER_PHRASES = (
    "بطاقه تحقيق",
    "تحقيق الشخصيه",
    "جمهوريه مصر",
    "مصر العربيه",
)
GOVERNORATE_PHRASES = sorted(
    {fold_arabic(name) for name in _GOVERNORATE_NAMES if fold_arabic(name)},
    key=len,
    reverse=True,
)


@contextmanager
def timed(label: str, bag: dict):
    start = time.perf_counter()
    try:
        yield
    finally:
        bag[label] = round((time.perf_counter() - start) * 1000, 1)


def analyze(doc_bytes: bytes, model_id: str, interval: float, timeout: float = DI_TIMEOUT_S) -> dict:
    deadline = time.monotonic() + timeout
    url = (f"{AZURE_DOC_INTEL_ENDPOINT}/documentintelligence/documentModels/"
           f"{model_id}:analyze?api-version={DI_API_VERSION}")
    if DI_FEATURES:
        url += f"&features={DI_FEATURES}"

    while True:
        resp = di_session.post(url, data=doc_bytes,
                               headers={"Content-Type": "application/octet-stream"},
                               timeout=timeout)
        if resp.status_code != 429:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("DI analyze request throttled until timeout")
        time.sleep(float(resp.headers.get("Retry-After", 1)))
    if resp.status_code != 202:
        raise RuntimeError(f"DI analyze request failed ({resp.status_code}): {resp.text}")
    operation_url = resp.headers["Operation-Location"]

    while True:
        time.sleep(interval)
        if time.monotonic() >= deadline:
            raise TimeoutError(f"DI analyze did not finish within {timeout}s")
        resp = di_session.get(operation_url, timeout=timeout)
        if resp.status_code == 429:
            time.sleep(float(resp.headers.get("Retry-After", 1)))
            continue
        resp.raise_for_status()
        body = resp.json()
        status = body.get("status")
        if status == "succeeded":
            return body["analyzeResult"]
        if status in ("failed", "canceled"):
            raise RuntimeError(f"DI analyze {status}: {json.dumps(body.get('error'))}")


def _to_jpeg(img) -> bytes:
    out = io.BytesIO()
    img.convert("RGB").save(out, format="JPEG", quality=90)
    return out.getvalue()


def prepare_image(image_bytes: bytes) -> bytes:
    """Apply the phone orientation, then shrink when a max side is configured."""
    if not image_bytes or image_bytes[:4] == b"%PDF":
        return image_bytes
    from PIL import Image, ImageOps
    with Image.open(io.BytesIO(image_bytes)) as img:
        img = ImageOps.exif_transpose(img)
        if IMAGE_MAX_SIDE_PX and max(img.size) > IMAGE_MAX_SIDE_PX:
            img.thumbnail((IMAGE_MAX_SIDE_PX, IMAGE_MAX_SIDE_PX))
        return _to_jpeg(img)


def _polygon_points(polygon) -> list[tuple[float, float]]:
    if not polygon:
        return []
    if isinstance(polygon[0], (int, float)):
        if len(polygon) < 4:
            return []
        return list(zip(polygon[0::2], polygon[1::2]))
    if isinstance(polygon[0], dict):
        return [(float(point.get("x", 0)), float(point.get("y", 0))) for point in polygon]
    return []


def _horizontal_delta(angle: float) -> float:
    """Degrees to rotate so a text line becomes horizontal. Range is -90 to 90."""
    normalized = (angle + 180) % 360 - 180
    if normalized > 90:
        normalized -= 180
    elif normalized < -90:
        normalized += 180
    return normalized


def _baseline(polygon) -> tuple[float, float, float] | None:
    """Return the tilt, the length along the text, and the line thickness."""
    points = _polygon_points(polygon)
    if len(points) < 2:
        return None
    if len(points) >= 4:
        (x0, y0), (x1, y1), (x2, y2), (x3, y3) = points[:4]
        top = (x1 - x0, y1 - y0)
        bottom = (x2 - x3, y2 - y3)
        side = (x2 - x1, y2 - y1)
        dx, dy = top if math.hypot(*top) >= math.hypot(*bottom) else bottom
        thickness = math.hypot(*side)
    else:
        (x0, y0), (x1, y1) = points[:2]
        dx, dy = x1 - x0, y1 - y0
        thickness = 0.0
    length = math.hypot(dx, dy)
    if length < 1e-6:
        return None
    return _horizontal_delta(math.degrees(math.atan2(dy, dx))), length, thickness


def _median_skew(lines: list[dict]) -> float | None:
    angles = []
    for line in lines:
        angle = line.get("angle")
        span = line.get("span") or 0
        thickness = line.get("thickness") or 0
        if angle is None or span <= 0 or span < thickness * 1.8:
            continue
        angles.append(angle)
    if len(angles) < 2:
        return None
    return float(statistics.median(angles))


def rotate_level(image_bytes: bytes, lines: list[dict]) -> bytes | None:
    """Rotate a tilted card so its text lines sit on the horizontal."""
    if not image_bytes or image_bytes[:4] == b"%PDF":
        return None
    angle = _median_skew(lines)
    if angle is None or abs(angle) < LEVEL_MIN_DEGREES:
        return None
    from PIL import Image
    with Image.open(io.BytesIO(image_bytes)) as img:
        leveled = img.rotate(angle, expand=True, fillcolor=(255, 255, 255), resample=Image.BICUBIC)
        encoded = _to_jpeg(leveled)
    logging.info("Rotated ID image by %.1f degrees to level the text.", angle)
    return encoded


def save_data_to_db(data: dict, conn_str: str):
    sql_insert_query = """
        INSERT INTO dbo.EgyptianIDsData (
            first_name_ocr, last_name_ocr, address_line_1_ocr,
            address_line_2_ocr, national_id_ocr, date_of_birth_ocr
        )
        VALUES (?, ?, ?, ?, ?, ?);
    """
    values_to_insert = (
        data.get("first name"), data.get("last name"), data.get("address_line_1"),
        data.get("address_line_2"), data.get("national_id"), data.get("date_of_birth")
    )
    try:
        logging.info("Connecting to the database to save OCR data.")
        with pyodbc.connect(conn_str) as cnxn:
            with cnxn.cursor() as cursor:
                cursor.execute(sql_insert_query, values_to_insert)
                cnxn.commit()
        logging.info("Successfully saved OCR data to the database.")
    except pyodbc.Error as db_err:
        logging.error(f"Database Error: Failed to save OCR data. Error: {db_err}", exc_info=True)
        raise


def get_dob_from_id(national_id: str) -> str:
    if not (isinstance(national_id, str) and len(national_id) == 14 and national_id.isdigit()):
        return ""
    try:
        century_digit = national_id[0]
        year_yy, month_mm, day_dd = national_id[1:3], national_id[3:5], national_id[5:7]
        if century_digit == '2': year = "19" + year_yy
        elif century_digit == '3': year = "20" + year_yy
        else: return ""
        if not (1 <= int(month_mm) <= 12 and 1 <= int(day_dd) <= 31): return ""
        return f"{year}-{month_mm}-{day_dd}"
    except (ValueError, IndexError):
        return ""


def _line_box(polygon) -> dict | None:
    points = _polygon_points(polygon)
    if len(points) < 4:
        return None
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    top, bottom = min(ys), max(ys)
    left, right = min(xs), max(xs)
    baseline = _baseline(polygon)
    return {
        "left": left,
        "right": right,
        "top": top,
        "bottom": bottom,
        "y": (top + bottom) / 2,
        "height": max(bottom - top, 0.0),
        "angle": baseline[0] if baseline else None,
        "span": baseline[1] if baseline else None,
        "thickness": baseline[2] if baseline else None,
    }


def extract_text_from_id(image_data: bytes) -> tuple[list[dict], str]:
    try:
        result = analyze(image_data, DI_MODEL_ID, DI_POLL_INTERVAL)
        lines, national_id = [], None
        for page in result.get("pages", []):
            page_width = float(page.get("width") or 0)
            for line in page.get("lines", []):
                content = line.get("content") or ""
                box = _line_box(line.get("polygon") or line.get("boundingPolygon"))
                lines.append({
                    "content": content,
                    "page_width": page_width,
                    **(box or {
                        "left": None, "right": None, "top": None, "bottom": None,
                        "y": None, "height": None, "angle": None, "span": None, "thickness": None,
                    }),
                })
                if not national_id:
                    cleaned_content = content.replace(" ", "").translate(ARABIC_TO_ENGLISH_DIGITS)
                    match = re.search(r"\b\d{14}\b", cleaned_content)
                    if match:
                        original_blocks = content.split()
                        reversed_blocks = [block[::-1] for block in original_blocks if block.isdigit()]
                        joined_reversed = ''.join(reversed_blocks)
                        english_digits = joined_reversed.translate(ARABIC_TO_ENGLISH_DIGITS)
                        final_id_candidate = english_digits[::-1]
                        if len(final_id_candidate) == 14:
                            national_id = final_id_candidate
        return lines, national_id or ""
    except Exception as e:
        logging.error(f"Error in extract_text_from_id: {e}", exc_info=True)
        return [], ""


def _contains_governorate(folded_line: str) -> bool:
    for phrase in GOVERNORATE_PHRASES:
        pattern = rf"(?<![\u0600-\u06FF\d]){re.escape(phrase)}(?![\u0600-\u06FF])"
        if re.search(pattern, folded_line):
            return True
    return False


def _is_national_id_line(line: str, national_id: str) -> bool:
    digits = re.sub(r"\D", "", line.translate(ARABIC_TO_ENGLISH_DIGITS))
    words = [word for word in fold_arabic(line).split() if not word.isdigit()]
    label_only = not words or all(word in HEADER_WORDS for word in words)
    if national_id and national_id in digits and label_only:
        return True
    letters = re.findall(r"[\u0600-\u06FF]", fold_arabic(line))
    if len(letters) >= 3:
        return False
    return len(digits) >= 12


def _is_header_word(word: str) -> bool:
    if word in HEADER_WORDS:
        return True
    return word.startswith("ال") and word[2:] in HEADER_WORDS


def _is_header_line(folded_line: str) -> bool:
    words = folded_line.split()
    if not words:
        return True
    if all(_is_header_word(word) for word in words):
        return True
    if any(phrase in folded_line for phrase in _HEADER_PHRASES):
        return True
    # The calligraphy "جمهورية مصر العربية" is often misread, but "العربية" survives.
    if "العربيه" in words and ("قصر" in words or "مصر" in words or "جمهوريه" in words):
        return True
    return False


def _is_governorate_line(text: str) -> bool:
    folded = fold_arabic(text)
    return bool(folded) and _contains_governorate(folded) and not _is_header_line(folded)


def _classify_lines(raw_lines: list[dict], national_id: str) -> tuple[list[dict], list[dict]]:
    """Split OCR lines into the four data rows and the card header."""
    data, headers = [], []
    for raw in raw_lines:
        cleaned = re.sub(r"\s+", " ", (raw.get("content") or "").strip())
        if not cleaned:
            continue
        folded = fold_arabic(cleaned)
        if not folded:
            continue
        item = {**raw, "content": cleaned}
        if _is_national_id_line(cleaned, national_id):
            continue
        if _CARD_SERIAL.fullmatch(folded.replace(" ", "")):
            continue
        if _DATE_ONLY.fullmatch(folded.replace(" ", "")):
            continue
        if _is_header_line(folded) or folded in STANDALONE_LABELS:
            headers.append(item)
            continue
        if data and data[-1]["content"] == cleaned:
            continue
        data.append(item)
    return data, headers


def _text_column(lines: list[dict]) -> list[dict]:
    """Keep the right-aligned name and address stack. Drop the photo-side text."""
    usable = [line for line in lines if line.get("right") is not None and line.get("page_width")]
    if len(usable) < 2:
        return lines
    median_right = statistics.median(line["right"] for line in usable)
    width = statistics.median(line["page_width"] for line in usable) or 1
    column = [line for line in usable if abs(line["right"] - median_right) <= width * 0.22]
    return column or lines


def _estimate_pitch(lines: list[dict]) -> float | None:
    ordered = sorted((line for line in lines if line.get("y") is not None), key=lambda line: line["y"])
    gaps = [
        below["y"] - above["y"]
        for above, below in zip(ordered, ordered[1:])
        if below["y"] > above["y"]
    ]
    heights = [line["height"] for line in ordered if line.get("height")]
    height_pitch = statistics.median(heights) * 1.55 if heights else None
    if not gaps:
        return height_pitch
    smallest = min(gaps)
    normal = [gap for gap in gaps if gap <= smallest * 1.35]
    pitch = statistics.median(normal)
    if height_pitch and pitch > height_pitch * 1.65:
        return height_pitch
    return pitch


def _slot_for_line(line_y: float, anchor_y: float, pitch: float, anchor_slot: int) -> int | None:
    delta = (anchor_y - line_y) / pitch
    steps = int(round(delta))
    slot = anchor_slot - steps
    if slot < 0 or slot > 3:
        return None
    # The name rows sit farther from the governorate than the address, so the
    # same spacing error pushes them off the grid first.
    if slot == 0:
        limit = FIRST_NAME_SLOT_MATCH
    elif slot == 1:
        limit = LAST_NAME_SLOT_MATCH
    else:
        limit = SLOT_MATCH
    if abs(delta - steps) > limit:
        return None
    return slot


def _place_in_slots(lines: list[dict], anchor: dict, anchor_slot: int, pitch: float) -> list[str | None]:
    """Put each line in its own row. A line that misses its row is not moved."""
    slots: list[str | None] = [None, None, None, None]
    closeness = [None, None, None, None]
    for line in lines:
        if line.get("y") is None:
            continue
        delta = (anchor["y"] - line["y"]) / pitch
        slot = _slot_for_line(line["y"], anchor["y"], pitch, anchor_slot)
        if slot is None:
            continue
        distance = abs(delta - round(delta))
        if slots[slot] is None or distance < closeness[slot]:
            slots[slot] = line["content"]
            closeness[slot] = distance
    return slots


def _header_anchor(headers: list[dict], column: list[dict], pitch: float) -> dict | None:
    if not headers or not column or not column[0].get("page_width"):
        return None
    median_right = statistics.median(line["right"] for line in column if line.get("right") is not None)
    width = column[0]["page_width"] or 1
    aligned = [
        line for line in headers
        if line.get("bottom") is not None and line.get("right") is not None
        and abs(line["right"] - median_right) <= width * 0.22
    ]
    pool = aligned or [line for line in headers if line.get("bottom") is not None]
    if not pool:
        return None
    bottom = max(line["bottom"] for line in pool)
    return {"y": bottom + pitch * 0.65, "bottom": bottom}


def _fields_from_slots(slots: list[str | None]) -> tuple:
    """A governorate line can only be address 2. A header line can never be a field."""
    values = list(slots)
    governorate_line = None
    for index, value in enumerate(values):
        if not value:
            continue
        folded = fold_arabic(value)
        if _is_header_line(folded):
            values[index] = None
            continue
        if _contains_governorate(folded):
            governorate_line = value
            if index != 3:
                values[index] = None
    if values[3] and not _is_governorate_line(values[3]):
        values[3] = None
    if values[3] is None and governorate_line and _is_governorate_line(governorate_line):
        values[3] = governorate_line
    return tuple(values)


def _fields_from_lines(data: list[dict], headers: list[dict]) -> tuple:
    column = sorted(_text_column(data), key=lambda line: line.get("y") if line.get("y") is not None else 0)
    positioned = [line for line in column if line.get("y") is not None and line.get("height")]
    if len(positioned) != len(column) or not positioned:
        contents = [line["content"] for line in column]
        governorates = [text for text in contents if _is_governorate_line(text)]
        if len(contents) == 4 and governorates and _is_governorate_line(contents[3]):
            return _fields_from_slots(contents)
        address_2 = governorates[-1] if len(governorates) == 1 else None
        return None, None, None, address_2

    pitch = _estimate_pitch(positioned)
    governorate_lines = [line for line in positioned if _is_governorate_line(line["content"])]
    if governorate_lines and pitch:
        slots = _place_in_slots(positioned, governorate_lines[-1], 3, pitch)
        return _fields_from_slots(slots)

    anchor = _header_anchor(headers, positioned, pitch) if pitch else None
    if anchor and pitch:
        slots = _place_in_slots(positioned, anchor, 0, pitch)
        return _fields_from_slots(slots)

    return None, None, None, None


def structure_id_text(raw_lines: list[dict], national_id: str) -> dict:
    """Map each printed row to its own field.

    A blurred or missing row stays empty. The line below it is not moved up
    into that field. Address line 2 must be the governorate row. The date of
    birth comes only from the national ID, never from another printed date.
    """
    data, headers = _classify_lines(raw_lines, national_id)
    logging.info("Structured ID from %d OCR lines into %d data lines.", len(raw_lines), len(data))

    first, last, address_1, address_2 = _fields_from_lines(data, headers)

    if (
        first and last and len(last.split()) == 1
        and not _contains_governorate(fold_arabic(last))
    ):
        first, last = last, first

    return {
        "first name": first,
        "last name": last,
        "address_line_1": address_1,
        "address_line_2": address_2,
        "national_id": national_id,
    }


ID_FIELDS = (
    "first name",
    "last name",
    "address_line_1",
    "address_line_2",
    "national_id",
    "date_of_birth",
)


def _blank(value) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def unreadable_field_errors(data: dict) -> dict[str, str]:
    """One message per field that came back empty."""
    errors = {}
    for field in ID_FIELDS:
        if _blank(data.get(field)):
            data[field] = None
            errors[field] = (
                f"{field} can't be read. Please upload a better quality image or a clearer image."
            )
    return errors


@app.route(route="process_id")
@app.queue_output(arg_name="msg", queue_name="id-data-to-save", connection="AzureWebJobsStorage")
def process_id_and_queue(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    timings = {}
    try:
        image_data = req.get_body()
        if not image_data:
            return func.HttpResponse("Please pass image data in the request body.", status_code=400)

        with timed("preprocess", timings):
            image_data = prepare_image(image_data)

        with timed("di_analyze", timings):
            raw_lines, national_id = extract_text_from_id(image_data)
            leveled = rotate_level(image_data, raw_lines)
            if leveled is not None:
                raw_lines, national_id = extract_text_from_id(leveled)

        with timed("structure", timings):
            if raw_lines:
                structured_id_data = structure_id_text(raw_lines, national_id)
            else:
                structured_id_data = {field: None for field in ID_FIELDS}
        if not structured_id_data:
            return func.HttpResponse("Failed to structure extracted text.", status_code=500)

        dob = get_dob_from_id(structured_id_data.get("national_id", ""))
        structured_id_data["date_of_birth"] = dob if dob else None

        errors = unreadable_field_errors(structured_id_data)
        if errors:
            structured_id_data["errors"] = errors
            logging.info("Unreadable fields: %s", ", ".join(errors))
            return func.HttpResponse(
                body=json.dumps(structured_id_data, indent=4, ensure_ascii=False),
                status_code=422,
                mimetype="application/json"
            )

        msg.set(json.dumps(structured_id_data, ensure_ascii=False))

        return func.HttpResponse(
            body=json.dumps(structured_id_data, indent=4, ensure_ascii=False),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Unhandled error in process_id_and_queue: {e}", exc_info=True)
        return func.HttpResponse("An unexpected error occurred.", status_code=500)
    finally:
        logging.info("timing_ms %s", timings)


@app.queue_trigger(arg_name="msg", queue_name="id-data-to-save", connection="AzureWebJobsStorage")
def save_data_from_queue(msg: func.QueueMessage) -> None:
    logging.info('Python Queue trigger function started processing a message.')
    timings = {}
    try:
        message_body = msg.get_body().decode('utf-8')
        data = json.loads(message_body)
        logging.info(f"Received data from queue for National ID: {data.get('national_id')}")

        with timed("sql_write", timings):
            save_data_to_db(data, sql_connect_str)

        logging.info(f"Successfully saved data for National ID: {data.get('national_id')}")
    except Exception as e:
        logging.error(f"Error processing queue message: {e}", exc_info=True)
        raise
    finally:
        logging.info("timing_ms %s", timings)
