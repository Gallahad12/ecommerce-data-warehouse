select
    {{ dbt_utils.generate_surrogate_key(['products.product_id']) }} as product_key,
    products.product_id,
    coalesce(translation.category_name_english, products.category_name, 'unknown') as category_name,
    products.photo_count,
    products.weight_g,
    products.length_cm,
    products.height_cm,
    products.width_cm
from {{ ref('stg_products') }} as products
left join {{ ref('stg_product_category_translation') }} as translation
    on products.category_name = translation.category_name
