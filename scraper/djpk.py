
import re
import time
from io import BytesIO
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
from bs4 import BeautifulSoup

from utils.api import create_session, get_text
from utils.data import normalize_dataframe
from utils.file import save_parquet, dataframe_to_bytes
from utils.storage import get_minio_client, upload_bytes


class DJPKAPBD:
    """
    Scraper Belanja Bantuan Keuangan dalam APBD Pemprov
    Menurut Provinsi dari portal DJPK Kemenkeu.

    Indikator XFlask : 10229
    Sumber           : Kemenkeu / DJPK
    Nilai utama      : Anggaran/Pagu
    """

    BASE_URL = "https://djpk.kemenkeu.go.id/portal/data/apbd"

    ID_NAMA_DATA = 10229
    STORAGE_NAME = "belanja_bantuan_keuangan"
    BUCKET_NAME = "maganghub"

    PROJECT_ROOT = Path(__file__).resolve().parents[1]

    RAW_DIR = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "djpk"
        / STORAGE_NAME
    )

    FINAL_DIR = (
        PROJECT_ROOT
        / "data"
        / "final"
        / "djpk"
        / STORAGE_NAME
    )

    RAW_PREFIX = f"djpk/apbd/raw/{STORAGE_NAME}"
    FINAL_PREFIX = f"djpk/apbd/final/{STORAGE_NAME}"

    TARGET_KEYWORDS = (
        "belanja bantuan keuangan",
    )

    def __init__(
        self,
        start_year=2021,
        end_year=2026,
        request_delay=0.3,
    ):
        if start_year > end_year:
            raise ValueError(
                "start_year tidak boleh lebih besar dari end_year"
            )

        self.start_year = int(start_year)
        self.end_year = int(end_year)
        self.request_delay = request_delay

        self.session = create_session()

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Referer": self.BASE_URL,
        }

        self.minio_client = None

    # ==================================================
    # URL
    # ==================================================


    def create_url(self, kode_provinsi, tahun):
        params = {
            "periode": "12",
            "tahun": str(tahun),
            "provinsi": str(kode_provinsi),
            "pemda": "00",
        }

        return f"{self.BASE_URL}?{urlencode(params)}"


    # ==================================================
    # REQUEST
    # ==================================================

    def get_page(self, url):
        return get_text(
            session=self.session,
            url=url,
            headers=self.headers,
            timeout=60,
        )

    # ==================================================
    # CLEANING
    # ==================================================

    @staticmethod
    def clean_text(value):
        value = str(value or "")
        value = re.sub(r"\s+", " ", value)
        return value.strip()

    @staticmethod
    def parse_number(value):
        """
        Mendukung format angka Indonesia:
        128.570,00 -> 128570.00
        128570,00  -> 128570.00

        Nilai kosong atau tanda '-' menjadi None.
        """
        value = str(value or "").strip()
        value = value.replace("\xa0", "")
        value = value.replace("Rp", "").strip()

        if value.lower() in {
            "", "-", "—", "–", "n/a", "na", "null"
        }:
            return None

        value = re.sub(r"\s+", "", value)

        if "," in value:
            value = value.replace(".", "")
            value = value.replace(",", ".")

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    # ==================================================
    # PROVINCE DISCOVERY
    # ==================================================

    def get_provinces(self):
        """
        Membaca dropdown provinsi dari HTML halaman DJPK.

        Jika dropdown dibuat dengan JavaScript atau datanya
        berasal dari endpoint lain, metode ini akan gagal
        secara eksplisit dan perlu disesuaikan.
        """
        html = self.get_page(self.BASE_URL)
        soup = BeautifulSoup(html, "html.parser")

        provinces = {}

        for select in soup.find_all("select"):
            identifier = (
                f"{select.get('name', '')} "
                f"{select.get('id', '')}"
            ).lower()

            if "provinsi" not in identifier:
                continue

            for option in select.find_all("option"):
                code = self.clean_text(option.get("value", ""))
                name = self.clean_text(
                    option.get_text(" ", strip=True)
                )

                if not code or not name:
                    continue

                if (
                    code in {"-", "0", "--"}
                    or "pilih" in name.lower()
                ):
                    continue

                provinces[code] = {
                    "kode_provinsi": code,
                    "provinsi": name,
                }

        if not provinces:
            raise RuntimeError(
                "Dropdown provinsi tidak ditemukan di HTML. "
                "Periksa HTML aktual atau endpoint JavaScript DJPK."
            )

        result = list(provinces.values())

        print(f"Provinsi ditemukan: {len(result)}")
        return result

    # ==================================================
    # TABLE PARSING
    # ==================================================

    @staticmethod
    def _find_column(headers, keywords):
        """
        Mencari indeks kolom berdasarkan teks header.
        Mengembalikan None jika tidak ditemukan.
        """
        for index, header in enumerate(headers):
            normalized = header.casefold()

            if any(
                keyword in normalized
                for keyword in keywords
            ):
                return index

        return None

    def parse_table(self, html, province, year, url):
        soup = BeautifulSoup(html, "html.parser")
        target = "Belanja Bantuan Keuangan"
        records = []

        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"], recursive=False)

            values = [
                self.clean_text(cell.get_text(" ", strip=True))
                for cell in cells
            ]

            # DEBUG: tampilkan baris yang mengandung nama akun
            row_text = " ".join(values)

            if target.casefold() in row_text.casefold():
                print("[DEBUG ROW]", values)

            # Cari nama akun secara tepat
            target_index = next(
                (
                    i for i, value in enumerate(values)
                    if value.casefold() == target.casefold()
                ),
                None,
            )

            if target_index is None:
                continue

            anggaran_index = target_index + 1
            realisasi_index = target_index + 2

            if realisasi_index >= len(values):
                print("[SKIP] Kolom tidak lengkap:", values)
                continue

            # Ambil kedua nilai dari tabel DJPK
            anggaran_text = (
                values[anggaran_index]
                .replace(" M", "")
                .strip()
            )
            realisasi_text = (
                values[realisasi_index]
                .replace(" M", "")
                .strip()
            )

            anggaran = self.parse_number(anggaran_text)
            realisasi = self.parse_number(realisasi_text)

            if anggaran is None or realisasi is None:
                print("[SKIP] Angka tidak terbaca:", values)
                continue

            print(f"[MATCH] {province['provinsi']} {year}: {values}")
            print(f"[ANGGARAN/PAGU] {anggaran}")
            print(f"[REALISASI] {realisasi}")

            records.append({
                "id_nama_data": self.ID_NAMA_DATA,
                "indikator": (
                    "Belanja Bantuan Keuangan dalam APBD "
                    "Pemprov Menurut Provinsi"
                ),
                "kode_provinsi": province["kode_provinsi"],
                "provinsi": province["provinsi"],
                "tahun": int(year),

                # Nilai utama indikator: Anggaran/Pagu
                "anggaran_asal": anggaran,

                # Tetap disimpan sebagai informasi pembanding
                "realisasi_asal": realisasi,

                "satuan_asal": "Miliar Rupiah",
                "label_tabel": target,
                "url_sumber": url,
                "sumber": "DJPK Kemenkeu",
            })

        # Hilangkan record duplikat
        unique_records = {
            (
                r["kode_provinsi"],
                r["tahun"],
                r["anggaran_asal"],
                r["realisasi_asal"],
            ): r
            for r in records
        }

        if len(unique_records) > 1:
            raise ValueError(
                f"Nilai indikator ambigu untuk "
                f"{province['provinsi']} tahun {year}: "
                f"{list(unique_records.keys())}"
            )

        return list(unique_records.values())


    # ==================================================
    # SCRAPE ONE PROVINCE-YEAR
    # ==================================================

    def scrape_one(self, province, year):
        url = self.create_url(
            province["kode_provinsi"],
            year,
        )

        html = self.get_page(url)

        records = self.parse_table(
            html=html,
            province=province,
            year=year,
            url=url,
        )

        if not records:
            print(
                f"[NOT FOUND] {province['provinsi']} - {year}"
            )
            return None

        return records[0]

    # ==================================================
    # SCRAPE ALL
    # ==================================================

    def scrape(self, provinces=None):
        if provinces is None:
            provinces = self.get_provinces()

        records = []
        errors = []

        for year in range(
            self.start_year,
            self.end_year + 1,
        ):
            print(f"\n===== TAHUN {year} =====")

            for province in provinces:
                try:
                    record = self.scrape_one(
                        province,
                        year,
                    )

                    if record:
                        records.append(record)
                        print(
                            f"[OK] {province['provinsi']} | "
                            f"{year} | "
                            f"Anggaran={record['anggaran_asal']} | "
                            f"Realisasi={record['realisasi_asal']}"
                        )

                except Exception as exc:
                    errors.append({
                        "tahun": year,
                        "provinsi": province["provinsi"],
                        "error": str(exc),
                    })

                    print(
                        f"[ERROR] {province['provinsi']} | "
                        f"{year} | {exc}"
                    )

                time.sleep(self.request_delay)

        if not records:
            raise RuntimeError(
                "Tidak ada data berhasil diambil. "
                "Periksa HTML dan struktur tabel DJPK."
            )

        df_raw = pd.DataFrame(records)

        if errors:
            self.errors_df = pd.DataFrame(errors)
            print(
                f"\nJumlah kombinasi provinsi-tahun gagal: "
                f"{len(errors)}"
            )
        else:
            self.errors_df = pd.DataFrame(
                columns=["tahun", "provinsi", "error"]
            )

        return df_raw

    # ==================================================
    # SAVE RAW
    # ==================================================

    def save_raw(self, df_raw):
        self.RAW_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        saved_paths = []

        for year, df_year in df_raw.groupby("tahun"):
            year = int(year)

            local_path = (
                self.RAW_DIR
                / f"{self.STORAGE_NAME}_{year}_raw.parquet"
            )

            save_parquet(df_year, local_path)

            object_name = (
                f"{self.RAW_PREFIX}/"
                f"{self.STORAGE_NAME}_{year}_raw.parquet"
            )

            payload = dataframe_to_bytes(
                df_year,
                format="parquet",
            )

            upload_bytes(
                client=self.minio_client,
                bucket_name=self.BUCKET_NAME,
                data=payload,
                object_name=object_name,
                content_type="application/octet-stream",
            )

            saved_paths.append(str(local_path))

        return saved_paths

    # ==================================================
    # TRANSFORM FINAL
    # ==================================================

    def transform(self, df_raw):
        df = df_raw.copy()

        # Berdasarkan pemeriksaan silang sebelumnya, nilai
        # XFlask untuk indikator ini cocok dengan Anggaran/Pagu.
        #
        # PENTING: konversi ke Rp ribu di bawah hanya benar
        # jika nilai yang diambil dari DJPK memang dalam
        # miliar rupiah. Verifikasi satuan tabel sebelum produksi.
        df["data_y"] = df["anggaran_asal"] * 1_000_000

        df["data_x"] = pd.to_datetime(
            df["tahun"].astype(str) + "-12-31",
            errors="coerce",
        ).dt.strftime("%d-%m-%Y")

        df["satuan"] = "Rp ribu"
        df["institusi"] = (
            "Direktorat Jenderal Perimbangan Keuangan"
        )
        df["nama"] = (
            "Belanja Bantuan Keuangan dalam "
            "APBD Pemprov Menurut Provinsi"
        )
        df["nama_alias"] = df["nama"]
        df["frekuensi"] = "Tahunan"
        df["leveldata"] = "Provinsi"
        df["negara"] = "Indonesia"
        df["kota"] = pd.NA
        df["id_kota"] = pd.NA
        df["id_provinsi"] = pd.NA
        df["kode_bps"] = pd.NA

        # Bersihkan awalan "Provinsi " pada nama wilayah
        df["provinsi"] = (
            df["provinsi"]
            .astype("string")
            .str.replace(r"^Provinsi\s+", "", regex=True)
            .str.strip()
        )
        df["item"] = pd.NA
        df["ket_tambahan"] = (
            "Nilai Anggaran/Pagu DJPK"
        )
        df["date"] = pd.to_datetime(
            df["tahun"].astype(str) + "-12-31"
        )
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        df["id"] = pd.NA
        df["id_nama_data"] = self.ID_NAMA_DATA

        df["sumber"] = "Kemenkeu"
        df["indikator"] = (
            "Belanja Bantuan Keuangan dalam "
            "APBD Pemprov Menurut Provinsi"
        )
        df["nama_data_import"] = df["url_sumber"]

        df = normalize_dataframe(
            df,
            text_columns=[
                "provinsi",
                "indikator",
                "satuan",
                "sumber",
                "institusi",
                "nama",
                "nama_data_import",
                "nama_alias",
                "frekuensi",
                "leveldata",
                "negara",
            ],
            numeric_columns=["data_y"],
        )

        final_columns = [
            "data_x",
            "data_y",
            "date",
            "frekuensi",
            "id",
            "id_kota",
            "id_nama_data",
            "id_provinsi",
            "indikator",
            "institusi",
            "item",
            "ket_tambahan",
            "kode_bps",
            "kota",
            "leveldata",
            "nama",
            "nama_data_import",
            "nama_alias",
            "negara",
            "provinsi",
            "satuan",
            "sumber",
        ]

        df_final = df[final_columns].copy()

        df_final = df_final.drop_duplicates(
            subset=["provinsi", "data_x"],
            keep="last",
        )

        return df_final.reset_index(drop=True)

    # ==================================================
    # SAVE FINAL
    # ==================================================

    def save_final(self, df_final):
        self.FINAL_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        local_path = (
            self.FINAL_DIR
            / f"{self.STORAGE_NAME}.csv"
        )

        df_final.to_csv(
            local_path,
            index=False,
            encoding="utf-8-sig",
        )

        object_name = (
            f"{self.FINAL_PREFIX}/"
            f"{self.STORAGE_NAME}.csv"
        )

        payload = dataframe_to_bytes(
            df_final,
            format="csv",
        )

        upload_bytes(
            client=self.minio_client,
            bucket_name=self.BUCKET_NAME,
            data=payload,
            object_name=object_name,
            content_type="text/csv; charset=utf-8",
        )

        print(f"[FINAL SAVED] {local_path}")
        print(f"[MINIO SAVED] {object_name}")

        return local_path

    # ==================================================
    # PIPELINE
    # ==================================================

    def run(self, provinces=None):
        print("=" * 70)
        print("DJPK APBD SCRAPER")
        print("=" * 70)
        print(
            f"Periode: {self.start_year}-{self.end_year}"
        )

        self.minio_client = get_minio_client()

        df_raw = self.scrape(provinces=provinces)

        print("\nRAW SHAPE:", df_raw.shape)
        print(df_raw.head().to_string(index=False))

        self.save_raw(df_raw)

        df_final = self.transform(df_raw)

        print("\nFINAL SHAPE:", df_final.shape)
        print(df_final.head().to_string(index=False))

        self.save_final(df_final)

        if not self.errors_df.empty:
            error_path = (
                self.FINAL_DIR / "scraping_errors.csv"
            )
            error_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
            self.errors_df.to_csv(
                error_path,
                index=False,
                encoding="utf-8-sig",
            )
            print(f"[ERROR LOG] {error_path}")

        print("\nSCRAPING SELESAI")
        return df_final


if __name__ == "__main__":
    scraper = DJPKAPBD(
        start_year=2021,
        end_year=2026,
    )

    scraper.run()
