import json
from datetime import datetime
from botocore.exceptions import ClientError

class CheckpointManager:
    def __init__(self, s3_client, bucket: str, data_type: str):
        """
        data_type: "ohlc" | "trade" | "quote"
        """
        self.s3 = s3_client
        self.bucket = bucket
        self.data_type = data_type

    def get_ohlc_key(self, symbol: str, resolution: str, start: datetime, end: datetime) -> str:
        s_str = start.strftime("%Y%m%d")
        e_str = end.strftime("%Y%m%d")
        return f"_checkpoints/ohlc/{symbol}/{resolution}/{s_str}_{e_str}.done"

    def get_history_folder(self, symbol: str, start: datetime, end: datetime) -> str:
        s_str = start.strftime("%Y%m%d_%H%M%S")
        e_str = end.strftime("%Y%m%d_%H%M%S")
        return f"_checkpoints/{self.data_type}/{symbol}/{s_str}_{e_str}"

    def get_history_key(self, symbol: str, start: datetime, end: datetime, page_num: int) -> str:
        folder = self.get_history_folder(symbol, start, end)
        return f"{folder}/page_{page_num:04d}.done"

    def checkpoint_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def write_checkpoint(self, key: str, meta: dict):
        payload = json.dumps({
            **meta,
            "completed_at": datetime.utcnow().isoformat(),
        }).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=payload)

    def read_checkpoint(self, key: str) -> dict:
        response = self.s3.get_object(Bucket=self.bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))

    def list_all_keys(self, prefix: str) -> list[str]:
        keys = []
        kwargs = {"Bucket": self.bucket, "Prefix": prefix}
        while True:
            response = self.s3.list_objects_v2(**kwargs)
            for obj in response.get("Contents", []):
                keys.append(obj["Key"])
            if not response.get("IsTruncated"):
                break
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
        return keys

    def get_last_history_page(self, symbol: str, start: datetime, end: datetime) -> tuple[int, str | None]:
        """
        Finds the latest page checkpoint inside the history folder.
        Returns:
            (last_page_num, next_page_token) if checkpoints exist, or (0, None)
        """
        folder = self.get_history_folder(symbol, start, end)
        keys = self.list_all_keys(prefix=folder + "/")
        if not keys:
            return 0, None

        page_keys = []
        for k in keys:
            if k.endswith(".done"):
                filename = k.split("/")[-1]
                if filename.startswith("page_"):
                    try:
                        p_num = int(filename.replace("page_", "").replace(".done", ""))
                        page_keys.append((p_num, k))
                    except ValueError:
                        pass

        if not page_keys:
            return 0, None

        # Sort by page number ascending
        page_keys.sort(key=lambda x: x[0])
        last_page_num, last_key = page_keys[-1]

        try:
            meta = self.read_checkpoint(last_key)
            return last_page_num, meta.get("next_page_token")
        except Exception as e:
            print(f"  [WARN] Failed to read checkpoint {last_key}: {e}")
            return 0, None

    def clear_checkpoints_for_range(self, symbol: str, resolution: str | None, start: datetime, end: datetime):
        if self.data_type == "ohlc":
            if not resolution:
                raise ValueError("resolution is required for clearing ohlc checkpoints")
            prefix = f"_checkpoints/ohlc/{symbol}/{resolution}/"
            keys = self.list_all_keys(prefix=prefix)
            from_date = start.date()
            to_date = end.date()
            for key in keys:
                filename = key.split("/")[-1]
                if not filename.endswith(".done"):
                    continue
                parts = filename.replace(".done", "").split("_")
                if len(parts) == 2:
                    try:
                        chunk_start = datetime.strptime(parts[0], "%Y%m%d").date()
                        chunk_end = datetime.strptime(parts[1], "%Y%m%d").date()
                        # Check overlap: chunk_start <= to_date and chunk_end >= from_date
                        if chunk_start <= to_date and chunk_end >= from_date:
                            print(f"  [OVERWRITE] Deleting S3 checkpoint: {key}")
                            self.s3.delete_object(Bucket=self.bucket, Key=key)
                    except ValueError:
                        pass
        else:
            folder = self.get_history_folder(symbol, start, end)
            keys = self.list_all_keys(prefix=folder + "/")
            if keys:
                print(f"  [OVERWRITE] Deleting {len(keys)} S3 checkpoints under folder {folder}")
                for key in keys:
                    self.s3.delete_object(Bucket=self.bucket, Key=key)
