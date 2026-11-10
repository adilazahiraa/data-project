import io
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from utils.data import concat_dataframes
from utils.file import (
    save_parquet,
    save_csv,
    dataframe_to_parquet_bytes,
)
from utils.api import create_session as create_api_session, get
from utils.storage import (
    MINIO_BUCKET,
    get_minio_client,
    upload_bytes,
    upload_local_file,
)

load_dotenv(override=True)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(PROJECT_ROOT))


class OJKExcel:
    """
    Generic scraper untuk mekanisme Excel OJK.

    Class ini mewakili MEKANISME:
        OJK publication page
            -> Excel
            -> sheet
            -> parsing
            -> transform
            -> RAW

    BUKAN class untuk satu dataset.

    Dataset-specific configuration dikirim melalui __init__.
    """

    def __init__(
        self,
        start_year,
        end_year,
        id_nama_data,
        source_url,
        sheet_name,
        storage_name,
        file_name,
        indikator,
        satuan,
        frekuensi,
        leveldata,
        note=None,
        nama_data_import=None,
        publication_keywords=None,
        historical_publication_url_templates=None,
        source_name="Otoritas Jasa Keuangan (OJK)",
        location_column=1,
        indicator_column=None,
        id_nama_data_mapping=None,
        current_periods=3,
    ):
        # ====================================================
        # PERIOD
        # ====================================================

        self.start_year = int(start_year)
        self.end_year = int(end_year)
        self.id_nama_data = int(id_nama_data)

        # ====================================================
        # SOURCE
        # ====================================================

        self.source_url = source_url
        self.source_name = source_name

        self.publication_keywords = (
            publication_keywords or []
        )

        self.historical_publication_url_templates = (
            historical_publication_url_templates or []
        )

        # ====================================================
        # DATASET CONFIG
        # ====================================================

        self.sheet_name = str(sheet_name)

        self.storage_name = storage_name

        self.file_name = file_name

        self.indikator = indikator

        self.satuan = satuan

        self.frekuensi = frekuensi

        self.leveldata = leveldata

        self.note = note

        self.nama_data_import = nama_data_import

        # ====================================================
        # PARSING CONFIG
        # ====================================================

        # Format Excel OJK Fintech saat ini:
        #
        # col0 = group
        # col1 = provinsi
        #
        # Contoh:
        # a. Jawa
        # 1. Banten
        # 2. DKI Jakarta
        # 3. Jawa Barat

        self.location_column = int(
            location_column
        )

        # Optional mapping for one Excel/dataframe containing
        # multiple indicators that map to different id_nama_data.
        self.indicator_column = indicator_column
        self.id_nama_data_mapping = id_nama_data_mapping or {}
        self.current_periods = int(current_periods)

        # ====================================================
        # SESSION
        # ====================================================

        self.session = self.create_session()

        # ====================================================
        # RAW PATH
        # ====================================================

        self.raw_dir = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "ojk"
            / self.storage_name
        )

        self.raw_file = (
            self.raw_dir
            / f"{self.file_name}_raw.parquet"
        )

    # ========================================================
    # SESSION
    # ========================================================

    @staticmethod
    def create_session():
        session = create_api_session()

        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/154.0 Safari/537.36"
            )
        })

        return session

    # ========================================================
    # DISCOVERY
    # ========================================================

    def is_publication_link(
        self,
        text,
        href,
    ):
        combined = (
            f"{text} {href}"
        ).lower()

        if not self.publication_keywords:
            return False

        return any(
            keyword.lower() in combined
            for keyword in self.publication_keywords
        )

    def get_publication_pages(self):
        """
        Ambil halaman publikasi OJK.
        """

        publication_urls = set()

        visited_pages = set()

        pages_to_visit = [
            self.source_url
        ]

        while pages_to_visit:

            current_url = (
                pages_to_visit.pop(0)
            )

            if current_url in visited_pages:
                continue

            visited_pages.add(
                current_url
            )

            print(
                "\nPAGE:",
                current_url,
            )

            response = get(
                session=self.session,
                url=current_url,
                timeout=30,
            )

            soup = BeautifulSoup(
                response.text,
                "html.parser",
            )

            # ------------------------------------------------
            # PUBLICATION
            # ------------------------------------------------

            for link in soup.find_all(
                "a",
                href=True,
            ):

                raw_href = link.get(
                    "href",
                    "",
                ).strip()

                text = link.get_text(
                    " ",
                    strip=True,
                )

                if not raw_href:
                    continue

                if raw_href.lower().startswith(
                    "javascript:"
                ):
                    continue

                href = urljoin(
                    current_url,
                    raw_href,
                )

                if not self.is_publication_link(
                    text,
                    href,
                ):
                    continue

                combined = (
                    f"{text} {href}"
                ).lower()

                has_target_year = any(
                    str(year) in combined
                    for year in range(
                        self.start_year,
                        self.end_year + 1,
                    )
                )

                if has_target_year:
                    publication_urls.add(
                        href
                    )

            # ------------------------------------------------
            # PAGINATION
            # ------------------------------------------------

            for link in soup.find_all(
                "a",
                href=True,
            ):

                text = link.get_text(
                    " ",
                    strip=True,
                )

                if not text.isdigit():
                    continue

                page_number = int(text)

                if (
                    page_number < 1
                    or page_number > 100
                ):
                    continue

                raw_href = link.get(
                    "href",
                    "",
                ).strip()

                if not raw_href:
                    continue

                if raw_href.lower().startswith(
                    "javascript:"
                ):
                    continue

                href = urljoin(
                    current_url,
                    raw_href,
                )

                if (
                    href not in visited_pages
                    and href not in pages_to_visit
                ):
                    pages_to_visit.append(
                        href
                    )

        print(
            "\nHALAMAN PUBLIKASI DITEMUKAN:"
        )

        for url in sorted(
            publication_urls
        ):
            print(
                " -",
                url,
            )

        return sorted(
            publication_urls
        )

    def get_historical_publication_pages(self):
        """
        Fallback discovery untuk halaman publikasi historis OJK.

        Beberapa publikasi lama tidak lagi muncul pada pagination
        halaman utama, sehingga URL historis dicari dari template config.
        """

        publication_urls = set()

        if not self.historical_publication_url_templates:
            return []

        for year in range(
            self.start_year,
            self.end_year + 1,
        ):
            for template in self.historical_publication_url_templates:
                url = template.format(year=year)

                try:
                    response = self.session.get(
                        url,
                        timeout=30,
                    )

                    if response.status_code != 200:
                        continue

                    soup = BeautifulSoup(
                        response.text,
                        "html.parser",
                    )

                    page_text = soup.get_text(
                        " ",
                        strip=True,
                    ).lower()

                    if self.publication_keywords:
                        if not any(
                            keyword.lower() in page_text
                            for keyword in self.publication_keywords
                        ):
                            continue

                    publication_urls.add(url)

                except Exception as e:
                    print(
                        "GAGAL HISTORICAL DISCOVERY:",
                        url,
                        e,
                    )

        return sorted(publication_urls)

    def get_excel_links(
        self,
        publication_urls,
    ):
        """
        Ambil semua file Excel dari halaman publikasi.
        """

        excel_links = set()

        for publication_url in publication_urls:

            try:
                print(
                    "\nBUKA PUBLIKASI:",
                    publication_url,
                )

                response = get(
                    session=self.session,
                    url=publication_url,
                    timeout=30,
                )

                soup = BeautifulSoup(
                    response.text,
                    "html.parser",
                )

                for link in soup.find_all(
                    "a",
                    href=True,
                ):

                    href = urljoin(
                        publication_url,
                        link["href"],
                    )

                    if href.lower().endswith(
                        (
                            ".xlsx",
                            ".xls",
                        )
                    ):
                        excel_links.add(
                            href
                        )

            except Exception as e:

                print(
                    "GAGAL PUBLIKASI:",
                    publication_url,
                    e,
                )

        print(
            "\nJUMLAH EXCEL:",
            len(excel_links),
        )

        return sorted(
            excel_links
        )

    # ========================================================
    # EXTRACT RAW
    # ========================================================

    def get_raw_data(
        self,
        excel_url,
    ):
        """
        Download Excel dan baca sheet target.
        """

        print(
            "\nDOWNLOAD:",
            excel_url,
        )

        response = get(
            session=self.session,
            url=excel_url,
            timeout=120,
        )

        content = response.content

        try:
            xls = pd.ExcelFile(
                io.BytesIO(content)
            )

        except Exception as e:

            raise RuntimeError(
                f"Gagal membuka Excel: {e}"
            ) from e

        if self.sheet_name not in (
            xls.sheet_names
        ):

            print(
                f"SKIP: sheet "
                f"'{self.sheet_name}' "
                f"tidak ditemukan"
            )

            print(
                "SHEET TERSEDIA:",
                xls.sheet_names,
            )

            return None

        df = pd.read_excel(
            io.BytesIO(content),
            sheet_name=self.sheet_name,
            header=None,
        )

        return df

    # ========================================================
    # PARSING
    # ========================================================

    @staticmethod
    def normalize_location(
        value,
    ):
        if pd.isna(value):
            return None

        value = str(value).strip()

        value = re.sub(
            r"^\d+\.\s*",
            "",
            value,
        )

        if not value:
            return None

        # Baris agregat, bukan wilayah/provinsi.
        excluded_locations = {
            "jumlah",
            "total",
            "jumlah keseluruhan",
        }

        if value.lower() in excluded_locations:
            return None

        return value

    @staticmethod
    def parse_period(
        value,
    ):
        if pd.isna(value):
            return None

        if isinstance(
            value,
            pd.Timestamp,
        ):
            return value

        value = str(value).strip()

        if not value:
            return None

        value = value.lstrip(
            "*"
        ).strip()

        # Jan-22
        match = re.match(
            r"^([A-Za-z]{3})-(\d{2})$",
            value,
        )

        if match:

            month = pd.to_datetime(
                match.group(1),
                format="%b",
                errors="coerce",
            )

            if pd.isna(month):
                return None

            return pd.Timestamp(
                year=2000
                + int(match.group(2)),
                month=month.month,
                day=1,
            )

        parsed = pd.to_datetime(
            value,
            errors="coerce",
        )

        if pd.isna(parsed):
            return None

        return parsed

    def find_header_row(self, df):
        """
        Cari baris header berdasarkan:
        - adanya label lokasi/location di salah satu kolom
        - adanya minimal 2 kolom periode
        """

        for row_index in range(min(len(df), 15)):

            row_values = df.iloc[row_index].tolist()

            # Cari label lokasi/location di seluruh kolom
            has_location = any(
                not pd.isna(value)
                and (
                    "lokasi" in str(value).strip().lower()
                    or "location" in str(value).strip().lower()
                )
                for value in row_values
            )

            if not has_location:
                continue

            # Cari kolom periode di seluruh baris
            period_count = 0

            for value in row_values:
                period = self.parse_period(value)

                if period is not None:
                    period_count += 1

            if period_count >= 2:
                return row_index

        raise ValueError(
            "Header lokasi + periode tidak ditemukan."
        )

    def parse_wide_table(
        self,
        df,
    ):
        """
        Convert wide Excel:

            provinsi | Jan-22 | Feb-22 | ...

        menjadi:

            provinsi | data_x | data_y
        """

        header_row = (
            self.find_header_row(df)
        )

        print(
            "HEADER ROW:",
            header_row,
        )

        # ----------------------------------------------------
        # LOCATION
        # ----------------------------------------------------

        location_column = (
            self.location_column
        )

        print(
            "LOCATION COLUMN:",
            location_column,
        )

        # ----------------------------------------------------
        # PERIOD
        # ----------------------------------------------------

        period_columns = {}

        for column_index in range(
            df.shape[1]
        ):

            period = self.parse_period(
                df.iloc[
                    header_row,
                    column_index,
                ]
            )

            if period is not None:

                period_columns[
                    column_index
                ] = period

        if not period_columns:

            raise ValueError(
                "Tidak ditemukan "
                "kolom periode."
            )

        print(
            "JUMLAH PERIODE DITEMUKAN:",
            len(period_columns),
        )

        # ----------------------------------------------------
        # PREVIEW
        # ----------------------------------------------------

        print(
            "\nCONTOH DATA MENTAH:"
        )

        print(
            df.iloc[
                2:8,
                :
            ].to_string()
        )

        # ----------------------------------------------------
        # PARSE ROW
        # ----------------------------------------------------

        rows = []

        for row_index in range(
            header_row + 1,
            len(df),
        ):

            location = (
                self.normalize_location(
                    df.iloc[
                        row_index,
                        location_column,
                    ]
                )
            )

            if not location:
                continue

            for (
                column_index,
                period,
            ) in period_columns.items():

                value = pd.to_numeric(
                    df.iloc[
                        row_index,
                        column_index,
                    ],
                    errors="coerce",
                )

                if pd.isna(value):
                    continue

                rows.append({
                    "provinsi": location,
                    "data_x": period,
                    "data_y": value,
                })

        return pd.DataFrame(
            rows,
            columns=[
                "provinsi",
                "data_x",
                "data_y",
            ],
        )

    # ========================================================
    # TRANSFORM
    # ========================================================

    def normalize_province_names(
        self,
        df,
    ):
        df = df.copy()

        replacements = {
            "Nangroe Aceh Darussalam":
                "Aceh",

            "Kepualauan Bangka Belitung":
                "Kepulauan Bangka Belitung",

            "Kep. Riau":
                "Kepulauan Riau",
        }

        df["provinsi"] = (
            df["provinsi"]
            .replace(replacements)
        )

        return df

    def apply_id_nama_data_mapping(
        self,
        df,
    ):
        """
        Apply id_nama_data mapping when one OJK dataframe contains
        multiple indicators.

        Example:
            {
                "Indikator A": 1,
                "Indikator B": 2,
                "Indikator C": 3,
            }

        If no indicator_column/mapping is configured, the configured
        self.id_nama_data is used for every row.
        """
        df = df.copy()

        if (
            self.indicator_column
            and self.indicator_column in df.columns
            and self.id_nama_data_mapping
        ):
            df["id_nama_data"] = (
                df[self.indicator_column]
                .astype(str)
                .str.strip()
                .map(self.id_nama_data_mapping)
            )

            # Keep only rows whose indicator has a configured mapping.
            df = df[df["id_nama_data"].notna()].copy()
            df["id_nama_data"] = df["id_nama_data"].astype(int)
        else:
            df["id_nama_data"] = self.id_nama_data

        return df

    def filter_current(
        self,
        df,
        periods=None,
    ):
        """
        Current-condition scenario.

        Only keeps the latest N available periods from the dataframe.
        For monthly data this normally means the latest 3 months.
        For quarterly/annual data it means the latest 3 observations.
        """
        df = df.copy()

        periods = int(periods or self.current_periods)

        if df.empty or "data_x" not in df.columns:
            return df

        df["data_x"] = pd.to_datetime(
            df["data_x"],
            errors="coerce",
        )

        df = df[df["data_x"].notna()].copy()

        if df.empty:
            return df

        latest_periods = (
            df["data_x"]
            .drop_duplicates()
            .sort_values(ascending=False)
            .head(periods)
            .tolist()
        )

        return (
            df[df["data_x"].isin(latest_periods)]
            .sort_values(["data_x", "provinsi"])
            .reset_index(drop=True)
        )

    def update_dataframe(
        self,
        df,
        backfill=True,
        current_periods=None,
    ):
        """
        Common update scenario used by both backfill and current update.

        backfill=True:
            Keep the configured historical range.

        backfill=False:
            Keep only the latest N periods.
        """
        df = self.transform(df)
        df = self.apply_id_nama_data_mapping(df)

        # Metadata wajib untuk FINAL / XFlask.
        df["satuan"] = self.satuan
        df["sumber"] = self.source_name
        df["nama_data_import"] = self.nama_data_import

        if not backfill:
            df = self.filter_current(
                df,
                periods=current_periods,
            )

        return df

    def get_update_year_range(
        self,
        backfill=True,
    ):
        """
        Return the year range used for discovery.

        Backfill:
            configured start_year -> end_year.

        Current:
            latest configured year and the two preceding years.
            This deliberately limits discovery to a small window so
            current update does not perform a full historical scrape.
        """
        if backfill:
            return self.start_year, self.end_year

        latest_year = int(self.end_year)
        return max(self.start_year, latest_year - 2), latest_year

    def filter_period(
        self,
        df,
    ):
        df = df.copy()

        df["data_x"] = pd.to_datetime(
            df["data_x"],
            errors="coerce",
        )

        df = df[
            df["data_x"].notna()
            & df["data_x"].dt.year.between(
                self.start_year,
                self.end_year,
            )
        ].copy()

        return df

    def transform(
        self,
        df,
    ):
        """
        Transform umum untuk mekanisme Excel OJK.

        Kalau dataset Excel lain membutuhkan
        transform khusus, method ini bisa
        dioverride/subclass.
        """

        df = (
            self.normalize_province_names(
                df
            )
        )

        df = (
            self.filter_period(df)
        )

        return df

    # ========================================================
    # LOAD RAW
    # ========================================================

    def get_yearly_raw_filename(self, year):
        return (
            self.raw_dir
            / f"{self.file_name}_{year}_.parquet"
        )


    def load_by_year(self, df):
        """
        Save RAW local sebagai Parquet per tahun.

        Contoh:
            dataset_2022_.parquet
            dataset_2023_.parquet
            dataset_2024_.parquet
            dataset_2025_.parquet
        """

        df = df.copy()

        df["data_x"] = pd.to_datetime(
            df["data_x"],
            errors="coerce",
        )

        saved_files = {}

        for year in sorted(
            df["data_x"].dt.year.dropna().unique()
        ):

            year = int(year)

            df_year = df[
                df["data_x"].dt.year == year
            ].copy()

            if df_year.empty:
                continue

            filename = self.get_yearly_raw_filename(
                year
            )

            save_parquet(
                df_year,
                filename,
            )

            saved_files[year] = filename

            print(
                "RAW SAVED:",
                filename,
                "| ROWS:",
                len(df_year),
            )

        return saved_files


    def load_minio_by_year(self, df):
        """
        Upload RAW Parquet ke MinIO per tahun.
        """

        df = df.copy()

        df["data_x"] = pd.to_datetime(
            df["data_x"],
            errors="coerce",
        )

        uploaded_files = {}

        client = get_minio_client()

        for year in sorted(
            df["data_x"].dt.year.dropna().unique()
        ):

            year = int(year)

            df_year = df[
                df["data_x"].dt.year == year
            ].copy()

            if df_year.empty:
                continue

            parquet_bytes = dataframe_to_parquet_bytes(
                df_year
            )

            filename = (
                f"{self.file_name}_{year}_.parquet"
            )

            object_name = (
                f"ojk/"
                f"{self.storage_name}/"
                f"raw/"
                f"{filename}"
            )

            upload_bytes(
                client,
                MINIO_BUCKET,
                parquet_bytes,
                object_name,
                "application/octet-stream",
            )

            uploaded_files[year] = object_name

            print(
                "MINIO RAW:",
                f"{MINIO_BUCKET}/{object_name}",
            )

        return uploaded_files


    def load_final_csv(self, df):
        """
        Save FINAL sebagai satu CSV gabungan
        seluruh tahun.
        """

        final_dir = (
            PROJECT_ROOT
            / "data"
            / "final"
            / "ojk"
        )

        final_file = (
            final_dir
            / f"{self.file_name}.csv"
        )

        # FINAL CSV mengikuti schema XFlask:
        # data_x harus DD-MM-YYYY.
        df_final = df.copy()

        df_final["data_x"] = pd.to_datetime(
            df_final["data_x"],
            errors="coerce",
        ).dt.strftime("%d-%m-%Y")

        save_csv(
            df_final,
            final_file,
            encoding="utf-8",
        )

        print(
            "FINAL CSV SAVED:",
            final_file,
        )

        # Upload FINAL CSV ke MinIO sesuai path nama_data_import:
        # ojk/{storage_name}/final/{file_name}.csv
        final_minio_path = None

        try:
            client = get_minio_client()

            final_object_name = (
                f"ojk/"
                f"{self.storage_name}/"
                f"final/"
                f"{self.file_name}.csv"
            )

            upload_local_file(
                client,
                MINIO_BUCKET,
                final_file,
                final_object_name,
                "text/csv",
            )

            final_minio_path = (
                f"{MINIO_BUCKET}/{final_object_name}"
            )

            print(
                "MINIO FINAL:",
                final_minio_path,
            )

        except Exception as e:
            print(
                "GAGAL UPLOAD FINAL MINIO:",
                e,
            )

        return final_file, final_minio_path

    # ========================================================
    # PROCESS
    # ========================================================

    def process_excel(
        self,
        excel_url,
    ):
        """
        Process satu Excel:

            download
                ↓
            read sheet
                ↓
            parse
                ↓
            transform
        """

        df = self.get_raw_data(
            excel_url
        )

        if df is None:
            return None

        print(
            "\nEXCEL:",
            excel_url,
        )

        print(
            "SHAPE:",
            df.shape,
        )

        parsed = (
            self.parse_wide_table(df)
        )

        print(
            "HASIL PARSE:",
            parsed.shape,
        )

        print(
            parsed.head()
        )

        if parsed.empty:
            return None

        parsed["source_url"] = (
            excel_url
        )

        parsed = self.update_dataframe(
            parsed,
            backfill=True,
        )

        return parsed

    # ========================================================
    # RUN
    # ========================================================

    def run(
        self,
        save_minio=False,
        backfill=True,
        current_periods=None,
    ):
        """
        Main orchestrator.

        backfill=True:
            Full configured historical range.

        backfill=False:
            Current-condition update. Discovery is limited to the
            latest configured year plus two previous years, then only
            the latest N available periods are returned.


        Flow:

            discovery
                ↓
            Excel links
                ↓
            extract
                ↓
            parse
                ↓
            transform
                ↓
            combine
                ↓
            dedup
                ↓
            save RAW
                ↓
            optional MinIO
        """

        print(
            "\n" + "=" * 70
        )

        print(
            "OJK EXCEL SCRAPER"
        )

        print(
            "=" * 70
        )

        print(
            "ID NAMA DATA:",
            self.id_nama_data,
        )

        print(
            "INDIKATOR:",
            self.indikator,
        )

        effective_start_year, effective_end_year = (
            self.get_update_year_range(backfill=backfill)
        )

        print(
            "MODE:",
            "BACKFILL" if backfill else "CURRENT",
        )
        print(
            "PERIODE DISCOVERY:",
            effective_start_year,
            "-",
            effective_end_year,
        )

        print(
            "SHEET:",
            self.sheet_name,
        )

        print(
            "STORAGE:",
            self.storage_name,
        )

        # For current update, narrow the discovery/filter window.
        original_start_year = self.start_year
        original_end_year = self.end_year
        self.start_year = effective_start_year
        self.end_year = effective_end_year

        # ----------------------------------------------------
        # DISCOVERY
        # ----------------------------------------------------

        publication_urls = (
            self.get_publication_pages()
        )

        historical_publication_urls = (
            self.get_historical_publication_pages()
        )

        publication_urls = sorted(
            set(publication_urls)
            | set(historical_publication_urls)
        )

        print(
            "\nTOTAL HALAMAN PUBLIKASI SETELAH HISTORICAL FALLBACK:",
            len(publication_urls),
        )

        excel_links = (
            self.get_excel_links(
                publication_urls
            )
        )

        # ----------------------------------------------------
        # EXTRACT
        # ----------------------------------------------------

        df_list = []

        failed_url = []

        for excel_url in excel_links:

            try:

                df = (
                    self.process_excel(
                        excel_url
                    )
                )

                if (
                    df is not None
                    and not df.empty
                ):

                    df_list.append(df)

                    print(
                        "SUCCESS:",
                        excel_url,
                    )

            except Exception as e:

                print(
                    "FAILED:",
                    excel_url,
                    e,
                )

                failed_url.append(
                    excel_url
                )

        # ----------------------------------------------------
        # VALIDATE
        # ----------------------------------------------------

        if not df_list:

            raise RuntimeError(
                "Tidak ada data OJK "
                "yang berhasil diproses."
            )

        # ----------------------------------------------------
        # COMBINE
        # ----------------------------------------------------

        df = concat_dataframes(
            df_list,
        )

        # ----------------------------------------------------
        # DEDUP
        # ----------------------------------------------------

        dedup_columns = [
            "provinsi",
            "data_x",
        ]

        df = (
            df
            .drop_duplicates(
                subset=dedup_columns + ["id_nama_data"],
                keep="last",
            )
            .sort_values(
                dedup_columns + ["id_nama_data"]
            )
            .reset_index(
                drop=True
            )
        )

        # Current-condition scenario:
        # latest value + previous N-1 periods.
        if not backfill:
            df = self.filter_current(
                df,
                periods=current_periods,
            )

        # Restore configured range on the scraper instance.
        self.start_year = original_start_year
        self.end_year = original_end_year

        # ----------------------------------------------------
        # DATE CREATED
        # ----------------------------------------------------

        date_created = (
            datetime.now().strftime(
                "%Y%m%d"
            )
        )

        # ----------------------------------------------------
        # FINAL INFO
        # ----------------------------------------------------

        print(
            "\n" + "=" * 70
        )

        print(
            "RAW FINAL"
        )

        print(
            "=" * 70
        )

        print(
            "ROWS:",
            len(df),
        )

        print(
            "PERIODE:",
            df["data_x"].min(),
            "s/d",
            df["data_x"].max(),
        )

        print(
            "FAILED:",
            len(failed_url),
        )

        # ----------------------------------------------------
        # SAVE RAW PER TAHUN
        # ----------------------------------------------------

        raw_paths = self.load_by_year(df)

        # ----------------------------------------------------
        # OPTIONAL MINIO
        # ----------------------------------------------------

        minio_paths = {}

        if save_minio:

            try:
                minio_paths = self.load_minio_by_year(
                    df
                )

            except Exception as e:
                print(
                    "GAGAL UPLOAD "
                    "RAW MINIO:",
                    e,
                )

        # ----------------------------------------------------
        # SAVE FINAL CSV
        # ----------------------------------------------------

        final_path, final_minio_path = (
            self.load_final_csv(df)
        )

        # ----------------------------------------------------
        # RESULT
        # ----------------------------------------------------

        return {
            "date_created": date_created,
            "success": df,
            "failed": failed_url,
            "raw_paths": raw_paths,
            "minio_paths": minio_paths,
            "final_path": final_path,
            "final_minio_path": final_minio_path,
        }


# ============================================================
# DATASET CONFIG
# ============================================================

FINTECH_CONFIG = {
    "start_year": 2022,
    "end_year": 2025,

    # Default id_nama_data for this dataset.
    # If one dataframe contains multiple indicators, use
    # indicator_column + id_nama_data_mapping instead.
    "id_nama_data": 14562,

    "source_url": (
        "https://ojk.go.id/id/kanal/iknb/"
        "data-dan-statistik/fintech/"
        "Default.aspx"
    ),

    "sheet_name": "18",

    "storage_name": (
        "akumulasi_penyaluran_pinjaman_fintech"
    ),

    "file_name": (
        "akumulasi_penyaluran_pinjaman_fintech_"
        "menurut_provinsi"
    ),

    "indikator": (
        "Akumulasi Penyaluran Pinjaman "
        "Fintech Menurut Provinsi"
    ),

    "satuan": "Rp miliar",
    "frekuensi": "bulan",
    "leveldata": "provinsi",

    "note": (
        "Data akumulasi penyaluran "
        "pinjaman fintech lending "
        "menurut provinsi berdasarkan "
        "statistik OJK periode bulanan."
    ),

    "nama_data_import": (
        "s3://maganghub/ojk/"
        "akumulasi_penyaluran_pinjaman_fintech/"
        "final/"
        "akumulasi_penyaluran_pinjaman_fintech_"
        "menurut_provinsi.csv"
    ),

    "publication_keywords": [
        "statistik lpbbti",
        "statistik p2p lending",
    ],

    # Publikasi historis yang kadang tidak lagi muncul
    # pada pagination halaman utama OJK.
    "historical_publication_url_templates": [
        (
            "https://ojk.go.id/id/kanal/iknb/"
            "data-dan-statistik/fintech/Pages/"
            "Statistik-Fintech-Lending-Periode-Desember-{year}.aspx"
        ),
        (
            "https://ojk.go.id/id/kanal/iknb/"
            "data-dan-statistik/fintech/Pages/"
            "Statistik-P2P-Lending-Periode-Desember-{year}.aspx"
        ),
    ],

    # Format Excel OJK:
    # col0 = group
    # col1 = province
    "location_column": 1,

    # Current-condition scenario:
    # latest period + 2 previous periods.
    "current_periods": 3,

    # Example for a dataset with multiple indicators:
    #
    # "indicator_column": "indikator",
    # "id_nama_data_mapping": {
    #     "Indikator 1": 10001,
    #     "Indikator 2": 10002,
    #     "Indikator 3": 10003,
    # },
    #
    # Leave both None/empty for a single id_nama_data dataset.
    "indicator_column": None,
    "id_nama_data_mapping": {},
}


# ============================================================
# UPDATE SCENARIO
# ============================================================

def update_idnamadata_akumulasi_penyaluran_pinjaman_fintech_menurut_provinsi(
    idnamdata=None,
    iditem=None,
    backfill=True,
    save_minio=False,
):
    """
    Dataset update entry point untuk indikator:
        "Akumulasi Penyaluran Pinjaman Fintech Menurut Provinsi".

    Scenario Mas Agus:
    - backfill=True  -> tarik seluruh histori sesuai konfigurasi.
    - backfill=False -> current condition:
                        discovery maksimal 2 tahun ke belakang
                        + ambil latest 3 periode.

    iditem disediakan agar signature bisa dikembangkan jika
    dataset OJK nantinya memiliki beberapa item/sub-dataset.
    """
    config = FINTECH_CONFIG.copy()

    if idnamdata is not None:
        config["id_nama_data"] = int(idnamdata)

    scraper = OJKExcel(**config)

    return scraper.run(
        save_minio=save_minio,
        backfill=backfill,
        current_periods=config.get("current_periods", 3),
    )


def update_curr_akumulasi_penyaluran_pinjaman_fintech_menurut_provinsi(
    idnamdata=None,
    iditem=None,
    save_minio=False,
):
    """
    Current-condition update untuk indikator:
        "Akumulasi Penyaluran Pinjaman Fintech Menurut Provinsi".

    Sekali run:
        latest period + 2 periode sebelumnya.

    Untuk data bulanan berarti 3 bulan terakhir.
    """
    return update_idnamadata_akumulasi_penyaluran_pinjaman_fintech_menurut_provinsi(
        idnamdata=idnamdata,
        iditem=iditem,
        backfill=False,
        save_minio=save_minio,
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    scraper = OJKExcel(
        **FINTECH_CONFIG
    )

    result = scraper.run(
        save_minio=True,
    )

    print(
        "\n" + "=" * 70
    )

    print(
        "OJK SELESAI"
    )

    print(
        "=" * 70
    )

    print(
        "RAW ROW:",
        len(
            result["success"]
        ),
    )

    print(
        "FAILED:",
        len(
            result["failed"]
        ),
    )

    print(
        "\nRAW FILES:"
    )

    for year, path in result[
        "raw_paths"
    ].items():

        print(
            f"  {year}: {path}"
        )

    print(
        "\nFINAL CSV LOCAL:",
        result["final_path"],
    )

    print(
        "FINAL CSV MINIO:",
        result["final_minio_path"],
    )

    print(
        "\nMINIO RAW FILES:"
    )

    for year, path in result[
        "minio_paths"
    ].items():

        print(
            f"  {year}: {path}"
        )