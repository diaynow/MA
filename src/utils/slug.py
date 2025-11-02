from slugify import slugify


def generate_slug(title: str) -> str:
    """Generate URL-friendly slug from title"""
    return slugify(title, max_length=255, word_boundary=True, save_order=True)
