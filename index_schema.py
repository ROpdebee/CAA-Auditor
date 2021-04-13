INDEX_SCHEMA = {
    'release': str,
    'images': [{
        'approved': bool,
        'back': bool,
        'comment': str,
        'edit': int,
        'front': bool,
        'id': int,
        'image': str,
        'thumbnails': {
            '250': str,
            '500': str,
            '1200': str,
            'small': str,
            'large': str,
        },
        'types': [str],
    }],
}
