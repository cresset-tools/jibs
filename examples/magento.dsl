# MySQL Import DSL Example - Magento Shop

# Variables
var base_domain: string
var base_port: int = 80
var admin_email: string = "admin@local.test"
var env: string = "development"
var skip_payments: bool = true
var order_limit: int = 100

# Faker data sources
faker names ["John", "Jane", "Bob", "Alice", "Charlie"]
faker emails ["user1@example.test", "user2@example.test", "user3@example.test"]
faker phones ["+31600000001", "+31600000002", "+31600000003"]

# Soft relations
relation customer_entity.group_id -> customer_group.customer_group_id
relation sales_order.customer_id -> customer_entity.entity_id

# Anonymization rules
anonymize customer_entity {
    email -> emails
    firstname -> names
    lastname -> names
    password -> null
}

anonymize sales_order_address {
    email -> emails
    firstname -> names
    lastname -> names
    telephone -> phones
}

# Table handling
ignore report_event
ignore sales_bestsellers_aggregated_daily

#[when($skip_payments)]
exclude sales_order_payment

# Aggregates
aggregate orders {
    root sales_order
    where "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"
    order by created_at desc
    limit 100
}

aggregate products {
    root catalog_product_entity
    where "entity_id IN (SELECT product_id FROM catalog_category_product WHERE category_id = 42)"
}

# Incremental imports
include products where "sku = 'HERO-PRODUCT'"

# Preserve local values
preserve core_config_data where "path LIKE 'dev/%'"

# Set values with string interpolation
set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = "https://{$base_domain}/"
}

set core_config_data {
    match path = "web/unsecure/base_url", scope = "default", scope_id = 0
    value = "http://{$base_domain}:{$base_port}/"
}

set core_config_data {
    match path = "web/cookie/cookie_domain", scope = "default", scope_id = 0
    value = ".{$base_domain}"
}

set core_config_data {
    match path = "trans_email/ident_general/email", scope = "default", scope_id = 0
    value = $admin_email
}

# Post-import transformations
#[when($env == "development")]
after {
    """
    UPDATE sales_order
    SET created_at = DATE_ADD(created_at, INTERVAL 10 YEAR)
    """
}
