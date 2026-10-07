"""
Dataset publication module leveraging the modern kagglehub API.
Supports both local execution (via .env) and Kaggle Scheduled Notebook execution (via Kaggle Secrets).
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import kagglehub
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent


def setup_credentials():
    """Resolve Kaggle credentials from Kaggle Secrets or local .env / environment."""
    # 1. Try Kaggle Secrets (if running in Kaggle environment)
    try:
        from kaggle_secrets import UserSecretsClient
        secrets = UserSecretsClient()
        username = secrets.get_secret("KAGGLE_USERNAME")
        token = secrets.get_secret("KAGGLE_API_TOKEN") or secrets.get_secret("KAGGLE_KEY")
        if username and token:
            os.environ["KAGGLE_USERNAME"] = username
            os.environ["KAGGLE_KEY"] = token
            kagglehub.config.set_kaggle_credentials(username, token)
            return username
    except Exception:
        pass

    # 2. Try local .env or system environment variables
    load_dotenv(ROOT_DIR / ".env")
    username = os.getenv("KAGGLE_USERNAME")
    token = os.getenv("KAGGLE_API_TOKEN") or os.getenv("KAGGLE_KEY")
    if username and token:
        os.environ["KAGGLE_USERNAME"] = username
        os.environ["KAGGLE_KEY"] = token
        kagglehub.config.set_kaggle_credentials(username, token)
        return username

    print("Error: KAGGLE_USERNAME or KAGGLE_API_TOKEN (KAGGLE_KEY) not found.", file=sys.stderr)
    sys.exit(1)


def publish(dataset_dir: Path = None, version_notes: str = None, dataset_slug: str = "data-finance-us-kaggle"):
    """Upload dataset or new version to Kaggle via kagglehub."""
    username = setup_credentials()
    target_dir = str(dataset_dir or ROOT_DIR)
    handle = f"{username}/{dataset_slug}"

    if not version_notes:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        version_notes = f"Incremental lakehouse update {now_str}"

    ignore_patterns = [
        "*.pyc",
        "*__pycache__*",
        "*.venv*",
        "*.git*",
        "*.tmp",
        "*.env*",
        "*data_analyze*",
        "*.ipynb_checkpoints*",
    ]

    print(f"[Kagglehub] Uploading dataset to {handle} from {target_dir}...")
    kagglehub.dataset_upload(
        handle=handle,
        local_dataset_dir=target_dir,
        version_notes=version_notes,
        ignore_patterns=ignore_patterns,
    )
    print(f"[Kagglehub] Successfully uploaded dataset version to {handle}!")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Publish dataset to Kaggle via kagglehub")
    parser.add_argument("notes", nargs="?", default=None, help="Version notes")
    parser.add_argument("--slug", type=str, default="data-finance-us-kaggle", help="Dataset slug (default: data-finance-us-kaggle)")
    args = parser.parse_args()
    publish(version_notes=args.notes, dataset_slug=args.slug)
