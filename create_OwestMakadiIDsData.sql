CREATE TABLE dbo.EgyptianIDsData (
    id INT IDENTITY(1, 1) NOT NULL CONSTRAINT PK_EgyptianIDsData PRIMARY KEY,
    first_name_ocr NVARCHAR(200) NULL,
    last_name_ocr NVARCHAR(300) NULL,
    address_line_1_ocr NVARCHAR(300) NULL,
    address_line_2_ocr NVARCHAR(300) NULL,
    national_id_ocr NVARCHAR(14) NULL,
    date_of_birth_ocr DATE NULL,
    created_at DATETIME2 NOT NULL
        CONSTRAINT DF_EgyptianIDsData_created_at DEFAULT SYSUTCDATETIME()
);
