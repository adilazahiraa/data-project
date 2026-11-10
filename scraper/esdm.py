import io
import os
import re
import sys
from pathlib import Path

# Pastikan root project (D:\data-project) masuk ke Python path saat
# script dijalankan dengan `python scraper/esdm.py 304`.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import requests
import pdfplumber
import pymupdf
from dotenv import load_dotenv

from utils.api import create_session
from utils.storage import (
    get_minio_client,
    upload_bytes,
)


# ============================================================
# ENV
# ============================================================

load_dotenv(override=True)

KATADATA_API_KEY = os.getenv("KATADATA_API_KEY")

DATABASE_BASE_URL = "https://xflask.databoks.id/database"


# ============================================================
# CONFIG
# ============================================================

# ============================================================
# PATH ESDM
# ============================================================

MINIO_BUCKET = "maganghub"

ESDM_RAW_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "esdm"
    / "cadangan_minyak_bumi"
)

ESDM_FINAL_DIR = (
    PROJECT_ROOT
    / "final"
    / "esdm"
    / "cadangan_minyak_bumi"
)

ESDM_RAW_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

ESDM_FINAL_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

ESDM_RAW_MINIO_PREFIX = (
    "esdm/"
    "cadangan_minyak_bumi/"
    "raw/"
)

ESDM_FINAL_FILE = (
    ESDM_FINAL_DIR
    / "cadangan_minyak_bumi.csv"
)

ESDM_MINIO_OBJECT = (
    "esdm/"
    "cadangan_minyak_bumi/"
    "final/"
    "cadangan_minyak_bumi.csv"
)

ESDM_CONFIG = {
    4784: {
        "indikator": "Cadangan Minyak Bumi",
        "sumber": "Kementerian ESDM",
        "tahun_terbaru": 2025,
        "jumlah_tahun_scrape": 3,
        "default_satuan": "MSTB",
        "storage_name": "cadangan_minyak_bumi",
        "file_name": "cadangan_minyak_bumi",

        # Laporan Kinerja 2025 digunakan karena
        # laporan tersebut memuat data tahun 2024.
        "source_method": "local_laporan_kinerja_pdf",
        "local_pdf": Path(r"D:\data-project\Laporan Kinerja 2025.pdf"),
        "source_url": (
            "https://migas.esdm.go.id/cms/uploads/"
            "informasi-publik/Laporan-Kinerja/"
            "LAKIN-2025.pdf"
        ),
    },
    304: {
        "indikator": "Ekspor Gas Pipa",
        "sumber": "Kementerian ESDM",
        "tahun_terbaru": 2025,
        "storage_name": "ekspor_gas_pipa",
        "file_name": "ekspor_gas_pipa",
        "source_method": "local_handbook_pdf_collection",
        "handbook_dir": PROJECT_ROOT / "handbook",
        "output_file": (
            PROJECT_ROOT / "data" / "raw" / "esdm"
            / "ekspor_gas_pipa" / "ekspor_gas_pipa_handbook.csv"
        ),
        "minio_prefix": "esdm/ekspor_gas_pipa/",
    },
}

ITEM_MASTER_FILE = PROJECT_ROOT / "item.parquet"


# ============================================================
# CLASS 1 - LAPORAN KINERJA PDF SCRAPER (ID 4784)
# ============================================================

class ESDMLaporanKinerjaPDF:

    def __init__(
        self,
        source_url,
        jumlah_tahun=3,
        local_pdf=None,
    ):
        self.source_url = source_url
        self.jumlah_tahun = int(jumlah_tahun)
        self.local_pdf = Path(
            local_pdf or r"D:\data-project\Laporan Kinerja 2025.pdf"
        )

    # --------------------------------------------------------
    # DOWNLOAD PDF
    # --------------------------------------------------------

    def download_pdf(self):
        local_pdf = self.local_pdf

        print("\n" + "=" * 60)
        print("LOAD PDF ESDM - CADANGAN MINYAK BUMI")
        print("=" * 60)

        print("LOCAL PDF:")
        print(local_pdf)

        if not local_pdf.exists():
            raise FileNotFoundError(
                f"PDF laporan kinerja tidak ditemukan: {local_pdf}"
            )

        with open(local_pdf, "rb") as f:
            pdf_bytes = f.read()

        print(
            "PDF BERHASIL DILOAD:",
            f"{len(pdf_bytes):,}",
            "bytes",
        )

        return pdf_bytes

    # --------------------------------------------------------
    # EXTRACT TEXT
    # --------------------------------------------------------

    def extract_data(self):
        pdf_bytes = self.download_pdf()

        pdf = pymupdf.open(
            stream=pdf_bytes,
            filetype="pdf",
        )

        target_page = None

        for page in pdf:
            text = page.get_text("text")

            if (
                "Gambar 10 Cadangan Minyak Bumi" in text
                and "Potensial" in text
                and "Terbukti" in text
            ):
                target_page = page
                break

        if target_page is None:
            raise ValueError(
                "Halaman 'Gambar 10 Cadangan Minyak Bumi' tidak ditemukan."
            )

        text = target_page.get_text("text")

        print("\nTARGET PAGE FOUND")

        # Normalisasi whitespace
        lines = [
            line.strip()
            for line in text.splitlines()
            if line.strip()
        ]

        # Cari posisi label
        try:
            potensial_idx = lines.index("Potensial")
            terbukti_idx = lines.index("Terbukti")
        except ValueError:
            raise ValueError(
                "Label 'Potensial' atau 'Terbukti' tidak ditemukan."
            )

        # Urutan tahun pada chart
        tahun_chart = [
            "2021",
            "2022",
            "2023",
            "2024",
            "2025",
        ]

        # Ambil 5 angka setelah Potensial
        potensial_values = []

        for value in lines[potensial_idx + 1:]:
            if value.isdigit():
                potensial_values.append(int(value))

            if len(potensial_values) == 5:
                break

        # Ambil 5 angka setelah Terbukti
        terbukti_values = []

        for value in lines[terbukti_idx + 1:]:
            if value.isdigit():
                terbukti_values.append(int(value))

            if len(terbukti_values) == 5:
                break

        if len(potensial_values) != 5:
            raise ValueError(
                f"Data Potensial tidak lengkap: {potensial_values}"
            )

        if len(terbukti_values) != 5:
            raise ValueError(
                f"Data Terbukti tidak lengkap: {terbukti_values}"
            )

        # Ambil beberapa tahun terakhir
        tahun_tersedia = [
            int(tahun)
            for tahun in tahun_chart
        ]

        tahun_target = tahun_tersedia[-self.jumlah_tahun:]

        print("\nTAHUN YANG DI-SCRAPE:")
        print(tahun_target)

        data = []

        for tahun in tahun_target:
            tahun_idx = tahun_chart.index(str(tahun))

            potensial = potensial_values[tahun_idx]
            terbukti = terbukti_values[tahun_idx]

            print(f"\nTahun     : {tahun}")
            print(f"Potensial : {potensial} MSTB")
            print(f"Terbukti  : {terbukti} MSTB")

            data.append({
                "tahun": tahun,
                "potential": potensial,
                "proven": terbukti,
                "satuan": "MSTB",
            })

        return data

    # --------------------------------------------------------
    # PARSE DATA
    # --------------------------------------------------------

    def parse_data(
        self,
        extracted_data,
    ):

        print("\n" + "=" * 60)
        print("PARSE DATA")
        print("=" * 60)

        def parse_number(value):

            if value is None:
                return pd.NA

            value = str(value).strip()

            if value in {
                "",
                "...",
                "–",
                "-",
                "NA",
            }:
                return pd.NA

            try:
                return float(
                    value.replace(",", "")
                )
            except ValueError:
                raise ValueError(
                    f"Nilai tidak dapat diparse: {value}"
                )

        rows = []

        for data in extracted_data:

            tahun = int(data["tahun"])

            potential = parse_number(
                data["potential"]
            )

            proven = parse_number(
                data["proven"]
            )

            # XFlask menyimpan nilai dalam skala 1.000.000
            potential = potential * 1_000_000
            proven = proven * 1_000_000

            # Total = Terbukti + Potensial
            total = pd.NA

            if (
                pd.notna(potential)
                and pd.notna(proven)
            ):
                total = proven + potential

            data_x = pd.Timestamp(
                f"{tahun}-12-31"
            )

            rows.extend(
                [
                    {
                        "item": "Total",
                        "data_x": data_x,
                        "data_y": total,
                        "satuan": "MSTB",
                    },
                    {
                        "item": "Terbukti",
                        "data_x": data_x,
                        "data_y": proven,
                        "satuan": "MSTB",
                    },
                    {
                        "item": "Potensial",
                        "data_x": data_x,
                        "data_y": potential,
                        "satuan": "MSTB",
                    },
                ]
            )

        df = pd.DataFrame(rows)

        print("\nHASIL PARSE:")

        print(df.head().to_string(index=False))

        return df

    # --------------------------------------------------------
    # RUN SCRAPER
    # --------------------------------------------------------

    def run(self):

        print("\n" + "=" * 60)
        print("SCRAPE ESDM - CADANGAN MINYAK BUMI")
        print("=" * 60)

        extracted_data = self.extract_data()

        df = (
            self.parse_data(
                extracted_data
            )
        )

        tahun_files = {}

        for tahun, df_tahun in df.groupby(
            df["data_x"].dt.year
        ):

            raw_file = (
                ESDM_RAW_DIR
                / f"cadangan_minyak_bumi_{tahun}.parquet"
            )

            df_tahun.to_parquet(
                raw_file,
                index=False,
            )

            tahun_files[int(tahun)] = raw_file

            print(
                f"\nRAW {tahun} DISIMPAN:"
            )
            print(raw_file)

        print("\n" + "=" * 60)
        print("SCRAPING ESDM SELESAI")
        print("=" * 60)

        print(
            "TOTAL ROW:",
            len(df),
        )

        return df


# ============================================================
# CLASS 2 - HANDBOOK PDF SCRAPER (ID 304)
# ============================================================

class ESDMHandbookScraper:
    """Ekstraksi tabel Ekspor Gas Pipa dari PDF handbook ESDM lokal.

    Kelas ini terpisah dari scraper laporan kinerja karena sumber dan cara
    akuisisinya berbeda. RAW disimpan sebagai Parquet per tahun untuk audit;
    tidak menjalankan ETL/final maupun upload MinIO otomatis.
    """

    KEYWORDS = (
        "natural gas and lng supply and demand",
        "export gas pipa",
        "export pipeline gas",
        "gas utilization",
    )

    def __init__(
        self,
        handbook_dir=None,
        output_file=None,
        idnamadata=304,
        indikator="Ekspor Gas Pipa",
        source_name="Kementerian ESDM",
    ):
        self.handbook_dir = Path(
            handbook_dir or (PROJECT_ROOT / "handbook")
        )
        self.output_file = Path(
            output_file or (
                PROJECT_ROOT / "data" / "raw" / "esdm"
                / "ekspor_gas_pipa" / "ekspor_gas_pipa_handbook.csv"
            )
        )
        self.idnamadata = int(idnamadata)
        self.indikator = indikator
        self.source_name = source_name

    @staticmethod
    def _clean(value):
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @staticmethod
    def _number(value):
        value = str(value or "").strip()
        if not value or value in {"-", "–", "—", "...", "NA", "N/A"}:
            return None
        value = value.replace(",", "").replace(" ", "")
        try:
            number = float(value)
        except ValueError:
            return None
        if number.is_integer():
            return int(number)
        return number

    @staticmethod
    def _unit(header_text):
        text = re.sub(r"\s+", "", header_text.upper())
        for unit in ("MMSCF", "BSCF", "TBTU", "THOUSANDMMBTU", "MMBTU"):
            if unit in text:
                return unit
        return "PERLU_DICEK"

    def _table_info(self, table):
        """Deteksi kolom dari header multi-baris maupun header terpecah per sel."""
        if not table:
            return None

        header_rows = table[:8]
        max_cols = max((len(row) for row in header_rows if row), default=0)
        if max_cols == 0:
            return None

        # Gabungkan teks per kolom secara vertikal. Pada beberapa handbook,
        # header "Export Pipeline Gas" terpecah menjadi beberapa baris/sel.
        column_headers = []
        for col_idx in range(max_cols):
            parts = []
            for row in header_rows:
                if col_idx < len(row):
                    cell = self._clean(row[col_idx])
                    if cell and cell.lower() not in {"none", "nan"}:
                        parts.append(cell)
            column_headers.append(re.sub(r"\s+", " ", " ".join(parts)).lower())

        header_text = " ".join(
            self._clean(cell) for row in header_rows for cell in row
        )
        header_text = re.sub(r"\s+", " ", header_text)

        year_idx = None
        export_idx = None
        for idx, col_text in enumerate(column_headers):
            normalized = re.sub(r"[^a-z0-9]+", " ", col_text).strip()
            if year_idx is None and re.search(r"\byear\b", normalized):
                year_idx = idx
            if export_idx is None and (
                "export pipeline gas" in normalized
                or "export gas pipa" in normalized
                or ("export" in normalized and "pipeline" in normalized and "gas" in normalized)
            ):
                export_idx = idx

        # Fallback untuk tabel yang header Year terbaca utuh di satu sel.
        if year_idx is None or export_idx is None:
            for row in header_rows:
                for idx, cell in enumerate(row):
                    cell_norm = re.sub(r"\s+", " ", self._clean(cell).lower())
                    if year_idx is None and cell_norm == "year":
                        year_idx = idx
                    if export_idx is None and (
                        "export pipeline gas" in cell_norm
                        or "export gas pipa" in cell_norm
                    ):
                        export_idx = idx

        return {
            "header_text": header_text,
            "column_headers": column_headers,
            "export_idx": export_idx,
            "year_idx": year_idx,
            "unit": self._unit(header_text),
        }

    def _record(self, year, value, unit, pdf_path, page_number):
        return {
            "idnamadata": self.idnamadata,
            "indikator": self.indikator,
            "tahun": int(year),
            "data_y": value,
            "satuan": unit,
            "sumber": self.source_name,
            "edisi_handbook": pdf_path.stem,
            "file_sumber": pdf_path.name,
            "halaman_pdf": page_number,
        }

    def _extract_page_tables(self, page, pdf_path, page_number):
        """Ekstraksi untuk tabel yang memiliki Year dan kolom ekspor pada halaman sama."""
        records = []
        for table in (page.extract_tables() or []):
            info = self._table_info(table)
            if not info or info["export_idx"] is None or info["year_idx"] is None:
                continue
            year_idx = info["year_idx"]
            export_idx = info["export_idx"]
            for row in table:
                if max(year_idx, export_idx) >= len(row):
                    continue
                year = self._number(row[year_idx])
                value = self._number(row[export_idx])
                if year is None or value is None or not (1900 <= year <= 2100):
                    continue
                records.append(self._record(
                    year, value, info["unit"], pdf_path, page_number
                ))
        return records

    def extract_pdf(self, pdf_path):
        """Ekstrak tabel langsung atau tabel yang terbelah horizontal lintas halaman.

        Pada beberapa edisi, kolom Year berada di halaman kiri dan kolom
        Export Pipeline Gas berada di halaman kanan/halaman berikutnya. Untuk
        layout ini, tahun dan nilai dipasangkan hanya jika jumlah barisnya sama.
        """
        records = []
        with pdfplumber.open(str(pdf_path)) as pdf:
            # HANDLING KHUSUS HANDBOOK 2025:
            # Tabel Gas Utilization tercetak pada halaman buku 96-97 (PDF 112-113).
            # pdfplumber membaca baris data sebagai teks lengkap pada kedua halaman,
            # walaupun tabelnya terpecah secara horizontal. Urutan kolom setelah Year:
            # Gas Fuel, City Gas, Lifting, Fertilizer, Electricity, Refinery,
            # Industry, LNG Domestic, LPG Domestic, Export Pipeline Gas, Export LNG.
            # Karena itu nilai Export Pipeline Gas adalah angka ke-2 dari kanan.
            # Ambil dari teks halaman yang memiliki header target dan validasi jumlah
            # angka per baris agar tidak mengandalkan tabel parsial hasil extract_tables().

            direct_records = []

            for page_number, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                text_norm = re.sub(r"\s+", " ", text.lower())

                # Tabel Gas Utilization pada handbook 2025
                if (
                    page_number not in (112, 113)
                    and "export pipeline" not in text_norm
                ):
                    continue

                for line in text.splitlines():
                    match = re.match(r"^\s*(20\d{2})\s+(.+?)\s*$", line)

                    if not match:
                        continue

                    year = int(match.group(1))

                    # Ambil seluruh tahun yang tersedia,
                    # bukan hanya tahun 2025.
                    number_tokens = re.findall(
                        r"(?<![A-Za-z])\d[\d,]*(?:\.\d+)?",
                        match.group(2),
                    )

                    values = [
                        self._number(token)
                        for token in number_tokens
                    ]
                    values = [
                        value for value in values
                        if value is not None
                    ]

                    # Tabel memiliki 11 nilai numerik setelah tahun.
                    if len(values) != 11:
                        print(
                            f"  BARIS {year} HALAMAN PDF "
                            f"{page_number} DIABAIKAN: "
                            f"diharapkan 11 nilai, ditemukan "
                            f"{len(values)} | {line}"
                        )
                        continue

                    # Export Pipeline Gas adalah angka kedua dari kanan.
                    export_pipeline_gas = values[-2]

                    print(
                        f"  DATA TARGET DITEMUKAN: "
                        f"halaman PDF {page_number} | "
                        f"tahun={year} | "
                        f"Export Pipeline Gas={export_pipeline_gas}"
                    )

                    direct_records.append(
                        self._record(
                            year,
                            export_pipeline_gas,
                            "THOUSAND MMBTU",
                            pdf_path,
                            page_number,
                        )
                    )

            if direct_records:
                # Deduplikasi karena baris dapat terbaca
                # pada kedua halaman PDF.
                unique = {}

                for record in direct_records:
                    key = (
                        record["tahun"],
                        record["data_y"],
                        record["satuan"],
                    )
                    unique.setdefault(key, record)

                return list(unique.values())

            page_parts = []
            for page_number, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                text_norm = re.sub(r"\s+", " ", text.lower())
                tables = page.extract_tables() or []
                table_infos = [(table, self._table_info(table)) for table in tables]

                has_relevant_text = any(k in text_norm for k in self.KEYWORDS)
                has_target_header = any(
                    info and info["export_idx"] is not None
                    for _, info in table_infos
                )
                has_year_header = any(
                    info and info["year_idx"] is not None
                    for _, info in table_infos
                )
                if has_relevant_text or has_target_header:
                    print(
                        f"  Kandidat tabel: {pdf_path.name} "
                        f"| halaman PDF {page_number}"
                    )

                # Prioritas pertama: tabel lengkap dalam satu halaman.
                direct = self._extract_page_tables(page, pdf_path, page_number)
                if direct:
                    records.extend(direct)
                    continue

                # Simpan tahun dari bagian kiri tabel Gas Utilization.
                years = []
                if (
                    "gas utilization" in text_norm
                    or "utilization" in text_norm
                    or has_year_header
                ):
                    for table, info in table_infos:
                        if not info:
                            continue
                        yi = info["year_idx"]
                        # Fallback: header Year kadang tidak dikenali karena
                        # kolomnya terpotong, tetapi tahun tetap ada di kolom pertama.
                        candidate_indices = [yi] if yi is not None else [0]
                        for row in table:
                            year = None
                            for yi_candidate in candidate_indices:
                                if yi_candidate < len(row):
                                    year = self._number(row[yi_candidate])
                                    if year is not None and 2012 <= year <= 2035:
                                        break
                            if year is not None and 2012 <= year <= 2035:
                                years.append(int(year))
                    # Fallback kedua dari teks halaman, khusus tabel yang
                    # pdfplumber tidak membentuk sebagai tabel terstruktur.
                    if not years:
                        for line in text.splitlines():
                            match = re.match(r"^\s*(20(?:1[2-9]|2[0-9]|3[0-5]))(?:\s|$)", line)
                            if match:
                                years.append(int(match.group(1)))
                    years = list(dict.fromkeys(years))

                # Simpan nilai kolom ekspor dari bagian kanan tabel.
                # Pada handbook 2025, header target terdeteksi pada teks halaman,
                # tetapi _table_info() tidak selalu berhasil menentukan export_idx.
                has_export_text = (
                    "export pipeline gas" in text_norm
                    or "export gas pipa" in text_norm
                )
                values = []
                value_unit = "PERLU_DICEK"
                if has_target_header or has_export_text:
                    for table, info in table_infos:
                        if not info:
                            continue
                        ei = info["export_idx"]
                        if ei is not None:
                            value_unit = info["unit"]
                            for row in table:
                                if ei >= len(row):
                                    continue
                                value = self._number(row[ei])
                                if value is not None:
                                    values.append(value)
                        elif has_export_text:
                            # Fallback untuk halaman lanjutan yang headernya
                            # terpecah/tidak terdeteksi sebagai satu kolom.
                            # Export Pipeline Gas adalah angka kedua dari kanan
                            # pada baris data (sebelum Export LNG). Abaikan
                            # baris header dan hanya ambil baris berisi >=2 angka.
                            value_unit = self._unit(text_norm)
                            for row in table:
                                numeric_cells = [self._number(cell) for cell in row]
                                numeric_cells = [v for v in numeric_cells if v is not None]
                                if len(numeric_cells) >= 2:
                                    values.append(numeric_cells[-2])
                    # Fallback teks jika tabel tidak berhasil dipecah menjadi
                    # kolom. Ini hanya digunakan pada halaman yang menyebut
                    # kolom ekspor secara eksplisit.
                    if not values and has_export_text:
                        value_unit = self._unit(text_norm)
                        for line in text.splitlines():
                            nums = re.findall(r"(?<![A-Za-z])\d[\d,]*(?:\.\d+)?", line)
                            parsed = [self._number(n) for n in nums]
                            parsed = [v for v in parsed if v is not None]
                            if len(parsed) >= 2 and not any(2012 <= v <= 2035 for v in parsed):
                                values.append(parsed[-2])

                page_parts.append({
                    "page_number": page_number,
                    "years": years,
                    "values": values,
                    "unit": value_unit,
                    "has_target_header": has_target_header,
                    "has_year_header": has_year_header,
                    "text_norm": text_norm,
                })

            # Pasangkan tabel yang terbelah horizontal antarhalaman.
            # Cari daftar tahun sebelumnya yang paling dekat, tetapi hanya
            # dari halaman yang menunjukkan tabel Gas Utilization/Year.
            for idx, part in enumerate(page_parts):
                if not part["values"] or part["years"]:
                    continue

                previous = None
                for candidate in reversed(page_parts[:idx]):
                    if candidate["years"] and (
                        "gas utilization" in candidate["text_norm"]
                        or "utilization" in candidate["text_norm"]
                    ):
                        previous = candidate
                        break
                if previous is None:
                    continue

                years = previous["years"]
                values = part["values"]

                # Kadang ekstraksi tabel mengambil angka dari baris header/footer.
                # Jangan menebak pasangan; log jumlahnya agar kegagalan terlihat.
                if len(years) != len(values):
                    print(
                        f"  TIDAK DIPASANGKAN halaman {previous['page_number']}-"
                        f"{part['page_number']}: jumlah tahun={len(years)}, "
                        f"jumlah nilai ekspor={len(values)}"
                    )
                    continue

                print(
                    f"  TABEL TERBELAH: halaman {previous['page_number']} + "
                    f"{part['page_number']} | {len(years)} baris | "
                    f"satuan={part['unit']}"
                )
                for year, value in zip(years, values):
                    records.append(self._record(
                        year, value, part["unit"], pdf_path, part["page_number"]
                    ))

        return records

    def run(self):
        if not self.handbook_dir.exists():
            raise FileNotFoundError(
                f"Folder handbook tidak ditemukan: {self.handbook_dir}. "
                "Simpan PDF handbook ESDM 2012-2025 di folder tersebut."
            )

        # Baca seluruh PDF di folder handbook.
        pdf_files = sorted(
            path
            for path in self.handbook_dir.glob("*.pdf")
            if path.is_file()
        )

        if not pdf_files:
            raise FileNotFoundError(
                f"Tidak ada PDF di folder {self.handbook_dir}"
            )

        print("\n" + "=" * 65)
        print("ESDM HANDBOOK SCRAPER - ID 304 EKSPOR GAS PIPA (ALL PDF)")
        print("FOLDER:", self.handbook_dir)
        print("JUMLAH PDF:", len(pdf_files))
        print("=" * 65)

        records = []
        for pdf_path in pdf_files:
            print(f"\nMEMBACA: {pdf_path.name}")
            try:
                records.extend(self.extract_pdf(pdf_path))
            except Exception as exc:
                print(f"  GAGAL MEMBACA {pdf_path.name}: {exc}")

        df = pd.DataFrame(records)
        # Ambil data mulai tahun 2012.
        df["tahun"] = pd.to_numeric(df["tahun"], errors="coerce")
        df = df[df["tahun"] >= 2012].copy()
        if df.empty:
            print("\nBELUM ADA RECORD YANG BERHASIL DIEKSTRAK.")
            print("Parser tabel perlu disesuaikan dengan struktur PDF yang terbaca.")
            return df

        # Simpan seluruh observasi tahun yang berhasil diekstrak.
        df = df.copy()

        if df.empty:
            print("\nTidak ada record yang berhasil diekstrak.")
            return df

        # Simpan kandidat dari tiap edisi untuk audit. Jangan deduplikasi
        # hanya berdasarkan tahun karena setiap handbook adalah bukti sumber.
        df = (
            df.drop_duplicates(
                subset=["tahun", "data_y", "satuan", "edisi_handbook"]
            )
            .sort_values(["tahun", "edisi_handbook", "halaman_pdf"])
            .reset_index(drop=True)
        )

        # RAW Parquet per tahun, mengikuti pola template scraper OJK:
        # ekspor_gas_pipa_2012_.parquet, ekspor_gas_pipa_2013_.parquet, dst.
        raw_dir = self.output_file.parent
        raw_dir.mkdir(parents=True, exist_ok=True)
        file_stem = self.output_file.stem.replace("_handbook", "")
        saved_files = {}

        for year, df_year in df.groupby("tahun", sort=True):
            year = int(year)
            raw_file = raw_dir / f"{file_stem}_{year}_.parquet"
            df_year.to_parquet(raw_file, index=False)
            saved_files[year] = raw_file
            print(
                f"RAW SAVED: {raw_file} | ROWS: {len(df_year)}"
            )

        print("\nREKAP TAHUN:")
        print(df.groupby(["satuan"]).agg(
            jumlah_record=("tahun", "size"),
            tahun_awal=("tahun", "min"),
            tahun_akhir=("tahun", "max"),
        ).to_string())
        print("\nHASIL EKSTRAKSI:")
        print(df.to_string(index=False))

        print("\nTOTAL FILE RAW:", len(saved_files))
        print("FOLDER RAW:", raw_dir)

        # ====================================================
        # FINAL CSV - ID 304 EKSPOR GAS PIPA
        # ====================================================

        final_dir = (
            PROJECT_ROOT
            / "final"
            / "esdm"
            / "ekspor_gas_pipa"
            / "final"
        )
        final_dir.mkdir(parents=True, exist_ok=True)

        final_file = final_dir / "ekspor_gas_pipa.csv"

        df_final = df.copy()

        # Format sesuai struktur final ESDM
        df_final["idnamadata"] = self.idnamadata
        df_final["nama_indikator"] = self.indikator
        df_final["item"] = "Indonesia"
        df_final["id_item"] = pd.NA
        df_final["data_x"] = (
            pd.to_datetime(
                df_final["tahun"].astype(str) + "-12-31",
                format="%Y-%m-%d",
            ).dt.strftime("%d-%m-%Y")
        )
        df_final["sumber"] = self.source_name
        df_final["nama_data_import"] = df_final["file_sumber"]
        df_final["negara"] = "Indonesia"

        df_final = df_final[
            [
                "idnamadata",
                "nama_indikator",
                "item",
                "id_item",
                "data_x",
                "data_y",
                "satuan",
                "sumber",
                "nama_data_import",
                "negara",
            ]
        ]

        # Cek agar CSV tidak kosong
        if df_final.empty:
            raise RuntimeError(
                "Final CSV tidak dibuat karena data kosong."
            )

        # Simpan CSV lokal
        df_final.to_csv(
            final_file,
            index=False,
            encoding="utf-8-sig",
        )

        print("\nFINAL CSV BERHASIL DISIMPAN:")
        print(final_file)
        print(df_final.to_string(index=False))

        # ====================================================
        # UPLOAD FINAL CSV KE MINIO
        # ====================================================

        minio_object = (
            "esdm/ekspor_gas_pipa/final/ekspor_gas_pipa.csv"
        )

        with open(final_file, "rb") as file:
            final_bytes = file.read()

        minio_client = get_minio_client()

        upload_bytes(
            minio_client,
            MINIO_BUCKET,
            final_bytes,
            minio_object,
            "text/csv",
        )

        print("\nFINAL CSV MINIO UPLOAD BERHASIL:")
        print(f"s3://{MINIO_BUCKET}/{minio_object}")

        print("\nTOTAL FINAL ROW:", len(df_final))

        return df_final



# ============================================================
# CLASS 3 - ETL CADANGAN MINYAK BUMI (EXISTING)
# ============================================================

class ESDMETL:

    def __init__(
        self,
        id_nama_data,
        nama_data_import,
    ):

        self.id_nama_data = int(
            id_nama_data
        )

        self.nama_data_import = nama_data_import

        self.minio_client = (
            get_minio_client()
        )


        if not KATADATA_API_KEY:
            raise RuntimeError(
                "KATADATA_API_KEY "
                "tidak ditemukan di .env"
            )

        self.session = (
            create_session()
        )

        self.session.headers.update({
            "X-API-Key":
                KATADATA_API_KEY
        })

    # --------------------------------------------------------
    # GET EXISTING XFLASK
    # --------------------------------------------------------

    def get_existing_xflask_data(self):

        print("\n" + "=" * 60)
        print("GET EXISTING XFLASK")
        print("=" * 60)

        url = (
            f"https://xflask.databoks.id/api/getdata/"
            f"{self.id_nama_data}"
        )

        response = self.session.get(
            url,
            timeout=60,
        )

        print(
            "STATUS:",
            response.status_code,
        )

        response.raise_for_status()

        data = response.json()

        print("RESPONSE RECEIVED")

        if isinstance(data, list):
            df = pd.DataFrame(data)

        elif isinstance(data, dict):

            # Coba ambil data dari key yang umum
            possible_keys = [
                "data",
                "result",
                "results",
                "rows",
            ]

            data_rows = None

            for key in possible_keys:

                if (
                    key in data
                    and isinstance(
                        data[key],
                        list,
                    )
                ):
                    data_rows = data[key]
                    break

            if data_rows is None:
                raise RuntimeError(
                    "Response XFlask berupa dict, "
                    "tetapi list data tidak ditemukan. "
                    f"Keys: {list(data.keys())}"
                )

            df = pd.DataFrame(
                data_rows
            )

        else:

            raise RuntimeError(
                "Format response XFlask tidak dikenali."
            )

        print(
            "JUMLAH EXISTING:",
            len(df),
        )

        print(
            "KOLOM:",
            df.columns.tolist(),
        )

        return df

    # --------------------------------------------------------
    # NORMALIZE ITEM
    # --------------------------------------------------------

    @staticmethod
    def normalize_item(
        value,
    ):

        text = str(
            value
        ).strip().lower()

        text = re.sub(
            r"[^a-z0-9]+",
            " ",
            text,
        ).strip()

        aliases = {
            "total": "total",

            "terbukti": "terbukti",
            "proven": "terbukti",
            "proven reserves": "terbukti",

            "potensial": "potensial",
            "potential": "potensial",
            "potential reserves": "potensial",
        }

        return aliases.get(
            text,
            text,
        )

    def map_item_ids(self, df):
        df = df.copy()

        item_master = pd.read_parquet(
            ITEM_MASTER_FILE,
            columns=["id", "nama"],
        )

        item_master["item_match"] = (
            item_master["nama"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        df["item_match"] = (
            df["item"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        df = df.merge(
            item_master.rename(
                columns={
                    "id": "id_item",
                    "nama": "item_master",
                }
            ),
            on="item_match",
            how="left",
        )

        # Total tidak dikirim ke final
        missing = df[
            (df["item"] != "Total")
            & (df["id_item"].isna())
        ]

        if not missing.empty:
            raise RuntimeError(
                "Item tidak ditemukan di item.parquet:\n"
                + missing["item"]
                .drop_duplicates()
                .to_string(index=False)
            )

        df["id_item"] = (
            pd.to_numeric(
                df["id_item"],
                errors="coerce",
            )
            .astype("Int64")
        )

        print("\nMAPPING ITEM:")
        print(
            df[
                ["item", "id_item"]
            ]
            .drop_duplicates()
            .to_string(index=False)
        )

        return df

    # --------------------------------------------------------
    # LOAD MASTER ITEM
    # --------------------------------------------------------

    def load_item_master(self):
        print("\n" + "=" * 60)
        print("LOAD MASTER ITEM")
        print("=" * 60)

        if not ITEM_MASTER_FILE.exists():
            raise FileNotFoundError(
                f"File item master tidak ditemukan: "
                f"{ITEM_MASTER_FILE}"
            )

        df_item = pd.read_parquet(
            ITEM_MASTER_FILE
        )

        print("MASTER ITEM BERHASIL DIBACA:", len(df_item))

        required_columns = {
            "id",
            "nama",
        }

        missing_columns = (
            required_columns
            - set(df_item.columns)
        )

        if missing_columns:
            raise RuntimeError(
                "Kolom item.parquet tidak lengkap. "
                f"Kolom yang hilang: {missing_columns}"
            )

        print("KOLOM MASTER:", df_item.columns.tolist())

        df_item = df_item[
            ["id", "nama"]
        ].copy()

        print("FILTER KOLOM MASTER BERHASIL:", len(df_item))

        df_item["item_match"] = df_item["nama"].apply(self.normalize_item)

        print("NORMALIZE MASTER BERHASIL")

        print("DUPLIKAT MASTER:", df_item["item_match"].duplicated().sum())

        print("LOAD MASTER SELESAI")

        df_item["item_match"] = (
            df_item["nama"]
            .apply(self.normalize_item)
        )

        # Buang item yang tidak punya nama
        df_item = df_item[
            df_item["item_match"].notna()
        ].copy()

        print(
            "\nTOTAL MASTER ITEM:",
            len(df_item),
        )

        return df_item

    # --------------------------------------------------------
    # MATCH ITEM SCRAPING VS MASTER ITEM
    # --------------------------------------------------------

    def map_item_ids(self, df_scraping):
        print("\n" + "=" * 60)
        print("MATCH ITEM SCRAPING VS MASTER ITEM")
        print("=" * 60)

        df = df_scraping.copy()

        df["item_match"] = (
            df["item"]
            .apply(self.normalize_item)
        )

        df_item = self.load_item_master()

        print("RETURN DARI LOAD MASTER")
        print("SCRAPING ROW:", len(df))
        print("MASTER ROW:", len(df_item))

        mapping = df_item[
            [
                "item_match",
                "id",
                "nama",
            ]
        ].rename(
            columns={
                "id": "id_item",
                "nama": "item_master",
            }
        )

        # Total tidak perlu dimapping karena
        # tidak akan masuk ke data final
        df_non_total = df[
            df["item"] != "Total"
        ].copy()

        df_total = df[
            df["item"] == "Total"
        ].copy()

        # Mapping hanya untuk item selain Total
        df_non_total = df_non_total.merge(
            mapping,
            on="item_match",
            how="left",
        )

        # Total tetap dipertahankan untuk proses validasi,
        # tetapi tidak memiliki id_item
        df_total["id_item"] = pd.NA
        df_total["item_master"] = "(total)"

        df = pd.concat(
            [
                df_total,
                df_non_total,
            ],
            ignore_index=True,
        )

        # Total tidak wajib punya id_item
        missing_item_id = df[
            (df["item"] != "Total")
            & (df["id_item"].isna())
        ].copy()

        if not missing_item_id.empty:
            raise RuntimeError(
                "Ada item hasil scraping yang "
                "tidak ditemukan di item.parquet:\n"
                + missing_item_id[
                    ["item"]
                ]
                .drop_duplicates()
                .to_string(index=False)
            )

        # Pastikan id_item integer
        df["id_item"] = (
            pd.to_numeric(
                df["id_item"],
                errors="coerce",
            )
            .astype("Int64")
        )

        print("\nHASIL MAPPING ITEM:")
        print(
            df[
                [
                    "item",
                    "item_master",
                    "id_item",
                ]
            ]
            .drop_duplicates()
            .to_string(index=False)
        )

        return df

    # --------------------------------------------------------
    # MATCH SCRAPING VS XFLASK
    # --------------------------------------------------------

    def match_data(
        self,
        df_scraping,
        df_existing,
    ):
        print("\n" + "=" * 60)
        print("MATCH SCRAPING VS XFLASK")
        print("=" * 60)

        scraping = df_scraping.copy()
        existing = df_existing.copy()

        # --------------------------------------------------------
        # Normalize item
        # --------------------------------------------------------

        scraping["item_match"] = (
            scraping["item"]
            .apply(self.normalize_item)
        )

        existing["item_match"] = (
            existing["item"]
            .apply(self.normalize_item)
        )

        # --------------------------------------------------------
        # Convert date
        # --------------------------------------------------------

        scraping["data_x"] = pd.to_datetime(
            scraping["data_x"],
            errors="coerce",
        )

        existing["date"] = pd.to_datetime(
            existing["date"],
            errors="coerce",
        )

        # Tahun untuk matching
        scraping["tahun"] = (
            scraping["data_x"].dt.year
        )

        existing["tahun"] = (
            existing["date"].dt.year
        )

        # --------------------------------------------------------
        # Mapping existing XFlask
        # berdasarkan ITEM + TAHUN
        # --------------------------------------------------------

        mapping = existing[
            [
                "item_match",
                "item",
                "tahun",
                "data_y",
                "satuan",
            ]
        ].drop_duplicates(
            subset=[
                "item_match",
                "tahun",
            ]
        )

        mapping = mapping.rename(
            columns={
                "item": "item_xflask",
                "data_y": "data_y_xflask",
                "satuan": "satuan_xflask",
            }
        )

        # --------------------------------------------------------
        # Merge scraping vs XFlask
        # --------------------------------------------------------

        result = scraping.merge(
            mapping,
            on=[
                "item_match",
                "tahun",
            ],
            how="left",
        )

        # --------------------------------------------------------
        # Status match
        # --------------------------------------------------------

        result["status_match"] = (
            result["data_y_xflask"].notna()
            & result["satuan_xflask"].notna()
        )

        result["status_match"] = result[
            "status_match"
        ].map(
            {
                True: "MATCH",
                False: "BELUM ADA",
            }
        )

        print(
            "\nSTATUS MATCH:",
            result["status_match"]
            .value_counts()
            .to_dict()
        )

        print(
            "TOTAL HASIL MATCH:",
            len(result)
        )

        return result
    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    def validate_data(
        self,
        df_match,
    ):

        print("\n" + "=" * 60)
        print("VALIDASI DATA")
        print("=" * 60)

        # ----------------------------------------------------
        # VALIDASI DATA BARU
        # ----------------------------------------------------

        df_new = df_match[
            (df_match["status_match"] != "MATCH")
            & (df_match["item"] != "Total")
        ].copy()

        print("FILTER DATA BARU BERHASIL:", len(df_new))

        if not df_new.empty:
            print("\nℹ️ DATA BARU / BELUM ADA DI XFLASK:")
            print("JUMLAH DATA BARU:", len(df_new))
        else:
            print("✅ SEMUA DATA SUDAH ADA DI XFLASK")

        # ----------------------------------------------------
        # VALIDASI NILAI
        # ----------------------------------------------------

        df_valid = df_match[
            df_match["status_match"] == "MATCH"
        ].copy()

        print("FILTER DATA MATCH BERHASIL:", len(df_valid))
        print("CHECKPOINT 1")

        if not df_new.empty:
            print("\nℹ️ DATA BARU / BELUM ADA DI XFLASK:")
            print("JUMLAH DATA BARU:", len(df_new))
        else:
            print("✅ SEMUA DATA SUDAH ADA DI XFLASK")

        df_valid = df_match[
            df_match["status_match"] == "MATCH"
        ].copy()

        print("CHECKPOINT 2")
        print("FILTER DATA MATCH BERHASIL:", len(df_valid))
        print("KOLOM DATA MATCH:", df_valid.columns.tolist())

        nilai_scraping = (
            pd.to_numeric(
                df_valid["data_y"],
                errors="coerce",
            )
        )

        nilai_xflask = (
            pd.to_numeric(
                df_valid["data_y_xflask"],
                errors="coerce",
            )
        )

        mismatch_nilai = (
            nilai_scraping != nilai_xflask
        )

        if mismatch_nilai.any():

            print(
                "\n❌ NILAI TIDAK SAMA:"
            )

            print(
                df_valid.loc[
                    mismatch_nilai,
                    [
                        "item",
                        "tahun",
                        "data_y",
                        "data_y_xflask",
                    ],
                ].to_string(
                    index=False
                )
            )

            raise RuntimeError(
                "Ada nilai scraping yang "
                "berbeda dengan XFlask."
            )

        print(
            "✅ NILAI DATA SESUAI"
        )

        # ----------------------------------------------------
        # VALIDASI SATUAN
        # ----------------------------------------------------

        satuan_scraping = (
            df_valid["satuan"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )

        satuan_xflask = (
            df_valid["satuan_xflask"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )

        mismatch_satuan = (
            satuan_scraping
            != satuan_xflask
        )

        if mismatch_satuan.any():

            print(
                "\n❌ SATUAN TIDAK SAMA:"
            )

            print(
                df_valid.loc[
                    mismatch_satuan,
                    [
                        "item",
                        "tahun",
                        "satuan",
                        "satuan_xflask",
                    ],
                ].to_string(
                    index=False
                )
            )

            raise RuntimeError(
                "Ada satuan scraping yang "
                "berbeda dengan XFlask."
            )

        print(
            "✅ SATUAN SESUAI"
        )

        print(
            "\n✅ VALIDASI SELESAI"
        )

        return True

    # --------------------------------------------------------
    # TRANSFORM
    # --------------------------------------------------------

    def transform(
        self,
        df_match,
    ):

        print("\n" + "=" * 60)
        print("TRANSFORM DATA")
        print("=" * 60)

        # ----------------------------------------------------
        # COPY DATA HASIL MATCH
        # ----------------------------------------------------

        df_final = df_match.copy()

        # ----------------------------------------------------
        # FORMAT DATA_X
        # ----------------------------------------------------

        df_final["data_x"] = pd.to_datetime(
            df_final["data_x"],
            errors="coerce",
        )

        df_final = df_final[
            df_final["data_x"].notna()
        ].copy()

        # ----------------------------------------------------
        # AMBIL 3 PERIODE TERAKHIR
        # ----------------------------------------------------

        periode_terakhir = (
            df_final["data_x"]
            .dt.year
            .dropna()
            .drop_duplicates()
            .sort_values()
            .tail(3)
            .tolist()
        )

        print("\nPERIODE FINAL:")
        print(periode_terakhir)

        df_final = df_final[
            df_final["data_x"]
            .dt.year
            .isin(periode_terakhir)
        ].copy()

        # ----------------------------------------------------
        # BUANG ITEM TOTAL
        # ----------------------------------------------------

        df_final = df_final[
            df_final["item"] != "Total"
        ].copy()

        # ----------------------------------------------------
        # VALIDASI NILAI
        # ----------------------------------------------------

        df_final = df_final[
            df_final["data_y"].notna()
        ].copy()

        # ----------------------------------------------------
        # TAMBAHKAN METADATA
        # ----------------------------------------------------

        df_final["idnamadata"] = (
            self.id_nama_data
        )

        df_final["nama_indikator"] = (
            "Cadangan Minyak Bumi"
        )

        df_final["sumber"] = (
            "Kementerian ESDM"
        )

        df_final["nama_data_import"] = (
            self.nama_data_import
        )

        df_final["negara"] = "Indonesia"

        # ----------------------------------------------------
        # FORMAT TANGGAL FINAL
        # ----------------------------------------------------

        df_final["data_x"] = (
            df_final["data_x"]
            .dt.strftime("%d-%m-%Y")
        )

        # ----------------------------------------------------
        # KOLOM FINAL
        # ----------------------------------------------------

        df_final = df_final[
            [
                "idnamadata",
                "nama_indikator",
                "item",
                "id_item",
                "data_x",
                "data_y",
                "satuan",
                "sumber",
                "nama_data_import",
                "negara",
            ]
        ]

        # ----------------------------------------------------
        # SORT
        # ----------------------------------------------------

        df_final["_sort_date"] = pd.to_datetime(
            df_final["data_x"],
            format="%d-%m-%Y",
            errors="coerce",
        )

        df_final = (
            df_final
            .sort_values(
                [
                    "_sort_date",
                    "item",
                ]
            )
            .drop(
                columns="_sort_date"
            )
            .reset_index(drop=True)
        )

        # ----------------------------------------------------
        # CHECK FINAL
        # ----------------------------------------------------

        print("\nFINAL DATA:")
        print(
            df_final.to_string(index=False)
        )

        print(
            "\nPERIODE FINAL:",
            sorted(
                pd.to_datetime(
                    df_final["data_x"],
                    format="%d-%m-%Y",
                )
                .dt.year
                .unique()
                .tolist()
            )
        )

        print(
            "JUMLAH FINAL ROW:",
            len(df_final),
        )

        return df_final

    def save_final(
        self,
        df_final,
    ):
        print(
            "\n" + "=" * 60
        )
        print(
            "SAVE FINAL CSV"
        )
        print(
            "=" * 60
        )

        df_final.to_csv(
            ESDM_FINAL_FILE,
            index=False,
            encoding="utf-8-sig",
        )

        print(
            "\nFINAL FILE:"
        )

        print(
            ESDM_FINAL_FILE
        )

        print(
            "TOTAL ROW:",
            len(df_final),
        )

        return ESDM_FINAL_FILE

    def upload_minio(
        self,
    ):

        print("\n" + "=" * 60)
        print("UPLOAD MINIO")
        print("=" * 60)

        # ========================================================
        # UPLOAD RAW PARQUET PER TAHUN
        # ========================================================

        raw_files = sorted(
            ESDM_RAW_DIR.glob(
                "cadangan_minyak_bumi_*.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "File RAW Parquet tidak ditemukan."
            )

        raw_minio_paths = []

        for raw_file in raw_files:

            object_name = (
                f"{ESDM_RAW_MINIO_PREFIX}"
                f"{raw_file.name}"
            )

            with open(
                raw_file,
                "rb",
            ) as file:
                raw_bytes = file.read()

            upload_bytes(
                self.minio_client,
                MINIO_BUCKET,
                raw_bytes,
                object_name,
                "application/octet-stream",
            )

            raw_minio_path = (
                f"s3://{MINIO_BUCKET}/"
                f"{object_name}"
            )

            raw_minio_paths.append(
                raw_minio_path
            )

            print(
                "\nRAW PARQUET MINIO UPLOAD BERHASIL:"
            )
            print(
                f"{MINIO_BUCKET}/"
                f"{object_name}"
            )

        # ========================================================
        # UPLOAD FINAL CSV
        # ========================================================

        with open(
            ESDM_FINAL_FILE,
            "rb",
        ) as file:
            final_bytes = file.read()

        upload_bytes(
            self.minio_client,
            MINIO_BUCKET,
            final_bytes,
            ESDM_MINIO_OBJECT,
            "text/csv",
        )

        print(
            "\nFINAL CSV MINIO UPLOAD BERHASIL:"
        )

        print(
            f"{MINIO_BUCKET}/"
            f"{ESDM_MINIO_OBJECT}"
        )

        return {
            "raw": raw_minio_paths,
            "final": (
                f"s3://{MINIO_BUCKET}/"
                f"{ESDM_MINIO_OBJECT}"
            ),
        }

    # --------------------------------------------------------
    # RUN ETL
    # --------------------------------------------------------

    def run(
        self,
        scraper,
    ):

        print("\n" + "=" * 60)
        print("ETL CADANGAN MINYAK BUMI")
        print("=" * 60)

        # ====================================================
        # EXTRACT
        # ====================================================

        df_scraping = (
            scraper.run()
        )

        df_scraping = self.map_item_ids(
            df_scraping
        )

        # ====================================================
        # LOAD EXISTING XFLASK
        # ====================================================

        df_existing = (
            self.get_existing_xflask_data()
        )

        # ====================================================
        # MATCH
        # ====================================================

        df_match = (
            self.match_data(
                df_scraping,
                df_existing,
            )
        )

        # ====================================================
        # VALIDATE
        # ====================================================

        self.validate_data(
            df_match
        )

        # ====================================================
        # TRANSFORM
        # ====================================================

        df_final = (
            self.transform(
                df_match
            )
        )

        final_file = self.save_final(
            df_final
        )

        minio_path = self.upload_minio()

        print(
            "\nFINAL LOCAL:"
        )

        print(
            final_file
        )

        print(
            "\nFINAL MINIO:"
        )

        print(
            minio_path
        )

        print("\n" + "=" * 60)
        print("PIPELINE ESDM SELESAI")
        print("=" * 60)

        return df_final


def build_ekspor_gas_pipa_final_only():
    """Bangun FINAL dari RAW Parquet lokal tanpa membaca/scrape PDF.

    Nilai THOUSAND MMBTU dikonversi secara pendekatan ke MMSCF memakai faktor
    ESDM (1 MMSCF ~= 1.01 thousand MMBTU), agar satuan FINAL konsisten dengan XFlask.
    """
    raw_dir = PROJECT_ROOT / "data" / "raw" / "esdm" / "ekspor_gas_pipa"
    raw_files = sorted(raw_dir.glob("ekspor_gas_pipa_*_.parquet"))
    if not raw_files:
        raise FileNotFoundError(
            f"RAW Parquet tidak ditemukan: {raw_dir}. Pastikan data sudah pernah di-scrape."
        )

    frames = []
    for path in raw_files:
        part = pd.read_parquet(path)
        if not part.empty:
            frames.append(part)
    if not frames:
        raise RuntimeError("Semua RAW Parquet kosong.")

    df = pd.concat(frames, ignore_index=True)
    required = {"tahun", "data_y", "satuan", "file_sumber", "edisi_handbook"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Kolom RAW tidak lengkap: {sorted(missing)}")

    df["satuan_norm"] = (
        df["satuan"].astype(str).str.upper().str.replace(r"\s+", "", regex=True)
    )
    df["tahun"] = pd.to_numeric(df["tahun"], errors="coerce")
    # Paksa data_y menjadi float SEBELUM konversi satuan. RAW Parquet lama
    # bisa menyimpan kolom ini sebagai int64, sedangkan hasil pembagian
    # THOUSAND MMBTU / 1.01 berupa float; tanpa cast ini pandas akan error.
    df["data_y"] = pd.to_numeric(df["data_y"], errors="coerce").astype("float64")
    # XFlask untuk ID 304 mengharapkan MMSCF. RAW handbook lama dapat
    # memakai MMSCF, sedangkan edisi baru memakai THOUSAND MMBTU.
    # Faktor pendekatan dari tabel konversi ESDM: 1 billion cubic feet NG
    # ~= 1.01 trillion Btu, sehingga 1 MMSCF ~= 1.01 thousand MMBTU.
    # Karena itu THOUSAND MMBTU dikonversi ke MMSCF dengan membagi 1.01.
    # Ini konversi pendekatan resmi, bukan sekadar mengganti label satuan.
    df = df[
        df["satuan_norm"].isin({"MMSCF", "THOUSANDMMBTU"})
        & df["tahun"].notna()
        & df["data_y"].notna()
        & (df["tahun"] >= 2012)
    ].copy()
    if df.empty:
        raise RuntimeError(
            "Tidak ada data dengan satuan MMSCF atau THOUSAND MMBTU pada RAW Parquet."
        )

    mask_thousand_mmbtu = df["satuan_norm"].eq("THOUSANDMMBTU")
    df.loc[mask_thousand_mmbtu, "data_y"] = (
        df.loc[mask_thousand_mmbtu, "data_y"] / 1.01
    )
    df["satuan"] = "MMSCF"

    # Ambil edisi handbook terbaru per tahun setelah semua satuan dinormalisasi.
    df["edisi_tahun"] = pd.to_numeric(
        df["edisi_handbook"].astype(str).str.extract(r"((?:19|20)\d{2})")[0],
        errors="coerce",
    ).fillna(0)
    df = (
        df.sort_values(["tahun", "edisi_tahun", "file_sumber"], ascending=[True, False, False])
        .drop_duplicates(subset=["tahun"], keep="first")
        .sort_values("tahun")
        .copy()
    )

    final_dir = PROJECT_ROOT / "final" / "esdm" / "ekspor_gas_pipa" / "final"
    final_dir.mkdir(parents=True, exist_ok=True)
    final_file = final_dir / "ekspor_gas_pipa.csv"

    # Harus berupa object-storage path, bukan nama PDF lokal seperti *.pdf.
    raw_object_prefix = "s3://maganghub/esdm/ekspor_gas_pipa/raw/"
    final = pd.DataFrame({
        "idnamadata": 304,
        "nama_indikator": "Ekspor Gas Pipa",
        "item": "Indonesia",
        "id_item": pd.NA,
        "data_x": pd.to_datetime(
            df["tahun"].astype(int).astype(str) + "-12-31", errors="coerce"
        ).dt.strftime("%d-%m-%Y"),
        "data_y": df["data_y"].astype(float).values,
        "satuan": "MMSCF",
        "sumber": "Kementerian ESDM",
        "nama_data_import": raw_object_prefix,
        "negara": "Indonesia",
    })
    final = final[[
        "idnamadata", "nama_indikator", "item", "id_item", "data_x",
        "data_y", "satuan", "sumber", "nama_data_import", "negara",
    ]]

    if final.empty or final["data_x"].isna().any():
        raise RuntimeError("FINAL CSV kosong atau memiliki tanggal tidak valid.")
    if not final["satuan"].eq("MMSCF").all():
        raise RuntimeError("Validasi satuan FINAL gagal: semua baris harus MMSCF.")
    if not final["nama_data_import"].astype(str).str.startswith(("http://", "https://", "s3://", "tos://", "minio://", "/")).all():
        raise RuntimeError("Validasi nama_data_import gagal: harus berupa URL/path object storage.")

    final.to_csv(final_file, index=False, encoding="utf-8-sig")
    print("FINAL-ONLY: tidak membaca PDF dan tidak menjalankan scraper.")
    print(f"RAW lokal dibaca: {len(raw_files)} file")
    print(f"FINAL CSV: {final_file}")
    print(f"Jumlah baris FINAL: {len(final)}")
    print(final.to_string(index=False))

    # Upload RAW yang sudah ada ke MinIO; tidak melakukan scraping ulang.
    client = get_minio_client()
    for raw_file in raw_files:
        object_name = f"esdm/ekspor_gas_pipa/raw/{raw_file.name}"
        upload_bytes(
            client, MINIO_BUCKET, raw_file.read_bytes(),
            object_name, "application/octet-stream",
        )
        print(f"MINIO RAW: s3://{MINIO_BUCKET}/{object_name}")

    # Upload FINAL CSV secara terpisah.
    final_object = "esdm/ekspor_gas_pipa/final/ekspor_gas_pipa.csv"
    upload_bytes(client, MINIO_BUCKET, final_file.read_bytes(), final_object, "text/csv")
    print(f"MINIO FINAL: s3://{MINIO_BUCKET}/{final_object}")
    return final


def fetch(params=None):

    params = params or {}

    idnamadata = params.get("idnamadata")

    if idnamadata is not None:
        idnamadata = int(idnamadata)

    # CADANGAN MINYAK BUMI
    if idnamadata == 4784:

        config = ESDM_CONFIG[4784]

        scraper = ESDMLaporanKinerjaPDF(
            source_url=config["source_url"],
            jumlah_tahun=config["jumlah_tahun_scrape"],
            local_pdf=config["local_pdf"],
        )

        etl = ESDMETL(
            id_nama_data=4784,
            nama_data_import=config["source_url"],
        )

        return etl.run(scraper)

    # EKSPOR GAS PIPA - sumber PDF handbook, pipeline terpisah
    if idnamadata == 304:
        config = ESDM_CONFIG[304]
        scraper = ESDMHandbookScraper(
            handbook_dir=config["handbook_dir"],
            output_file=config["output_file"],
            idnamadata=idnamadata,
            indikator=config["indikator"],
            source_name=config["sumber"],
        )
        # Tahap ini hanya ekstraksi + RAW Parquet per tahun. Tidak mengubah
        # final Cadangan Minyak Bumi dan tidak upload ke MinIO otomatis.
        return scraper.run()

    raise ValueError(
        f"idnamadata ESDM tidak dikenali: {idnamadata}"
    )

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    import sys

    # Untuk membangun ulang FINAL CSV dari RAW saja:
    # python scraper/esdm.py 304 --final-only
    requested_id = int(sys.argv[1]) if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else 304
    # ID 304 default-nya hanya rebuild FINAL dari RAW; scrape PDF harus diminta eksplisit dengan --scrape.
    final_only = "--scrape" not in sys.argv[1:]

    if requested_id == 304 and final_only:
        result = build_ekspor_gas_pipa_final_only()
        print("\nTOTAL FINAL ROW:", len(result))
    elif requested_id == 304:
        result = fetch({"idnamadata": 304})
        print("\nTOTAL HASIL EKSTRAKSI:", len(result))
    elif requested_id == 4784:
        config = ESDM_CONFIG[4784]
        scraper = ESDMLaporanKinerjaPDF(
            source_url=config["source_url"],
            jumlah_tahun=config["jumlah_tahun_scrape"],
            local_pdf=config["local_pdf"],
        )
        etl = ESDMETL(
            id_nama_data=4784,
            nama_data_import=config["source_url"],
        )
        df_final = etl.run(scraper)
        print("\nTOTAL FINAL:", len(df_final))
    else:
        raise ValueError(f"ID ESDM belum didukung: {requested_id}")