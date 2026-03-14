from app import db
from app.models.user_models import StoredFile  # use your actual model name


def load_file_from_db(user_id, country, filename):
    return (
        db.session.query(StoredFile)
        .filter_by(user_id=user_id, country=country, filename=filename)
        .order_by(StoredFile.id.desc())
        .first()
    )


def save_file_to_db(user_id, country, filename, file_bytes, kind, month, year, content_type):
    row = StoredFile(
        user_id=user_id,
        country=country,
        filename=filename,
        data=file_bytes,   # ✅ correct column name
        kind=kind,
        month=month,
        year=year,
        content_type=content_type,
    )
    db.session.add(row)
    db.session.commit()
    return row