select
    product_id,
    product_category_name as category_name,
    cast(product_photos_qty as int) as photo_count,
    cast(product_weight_g as int) as weight_g,
    cast(product_length_cm as int) as length_cm,
    cast(product_height_cm as int) as height_cm,
    cast(product_width_cm as int) as width_cm
from {{ source('olist', 'products') }}
