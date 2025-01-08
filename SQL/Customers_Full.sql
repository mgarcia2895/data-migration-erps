--Customer data cleaned for data migration. Selected columns are made to populate sample template.
with unique_customers as (

    select distinct arcusto_id

    from fivetran_raw.oracle_iqms.orders

    union

    select distinct arcusto_id

    from fivetran_raw.oracle_iqms.hist_orders

    union

    select distinct arcusto_id

    from fivetran_raw.oracle_iqms.arinvoice

    union

    select distinct arcusto_id

    from fivetran_raw.oracle_iqms.rma

    union

    select distinct principle_source_id as arcusto_id

    from fivetran_raw.oracle_iqms.crm_activity

    where principle_source = 'ARCUSTO' and type = '4'

)

,unique_industries as (

    select distinct
        unique_customers.arcusto_id
        ,v.industry
        ,count(v.industry) as industry_count

    from unique_customers
    left outer join iqms.public.crm_opportunity c
        on unique_customers.arcusto_id = c.principle_source_id
    left outer join iqms.public.v_ud_crm_opportunity v
        on c.id = v.parent_id

    where c.principle_source = 'ARCUSTO' and v.industry is not null

    group by unique_customers.arcusto_id, v.industry

    having count(v.industry) = 1

    order by arcusto_id

)

,crm_opportunity as (

    select * from iqms.public.crm_opportunity c where principle_source = 'ARCUSTO'

)

,final as (
    select *
    from (
        select distinct
            unique_customers.arcusto_id                             as external_id
            ,arcusto.custno                                         as id
            ,initcap(lower(arcusto.company))                        as company_name
            ,'Wesley International LLC'                             as subsidiary_external_id
            ,null                                                   as parent_company_external_id
            ,null                                                   as category
            ,arcusto.cust_since                                     as customer_since
            ,null                                                   as sales_rep
            ,case
                -- Extract email from '<>' if it contains '@' and does not contain 'www.'
                when position('@' in lower(arcusto.prime_contact_email)) > 0
                    and position('www.' in lower(arcusto.prime_contact_email)) = 0
                    and position('<' in lower(arcusto.prime_contact_email)) > 0
                    and position('>' in lower(arcusto.prime_contact_email)) > 0
                then
                    substring(
                        lower(arcusto.prime_contact_email),
                        position('<' in lower(arcusto.prime_contact_email)) + 1,
                        position('>' in lower(arcusto.prime_contact_email)) - position(
                            '<' in lower(arcusto.prime_contact_email)) - 1
                    )

                -- Set to NULL if it contains 'www.' or is not a valid email
                when position('@' in lower(arcusto.prime_contact_email)) = 0
                    or position('www.' in lower(arcusto.prime_contact_email)) > 0
                then
                    null

                -- Otherwise, keep the email as is
                else lower(arcusto.prime_contact_email)
            end as email
            ,case
                -- If the number starts with "00" or "+", keep the full cleaned string
                when regexp_like(arcusto.phone_number, '^(\\+|00)') then
                    regexp_replace(arcusto.phone_number, '[^0-9]', '')
                -- remove non-digit characters, keep full number
                -- For standard North American numbers, keep only the first 10 digits
                when length(regexp_replace(arcusto.phone_number, '[^0-9]', '')) > 10 then
                    substr(
                        case
                            when substr(regexp_replace(arcusto.phone_number, '[^0-9]', ''), 1, 1) = '1'
                            then substr(regexp_replace(arcusto.phone_number, '[^0-9]', ''), 2)
                            else regexp_replace(arcusto.phone_number, '[^0-9]', '')
                        end, 1, 10
                    )
                else
                    regexp_replace(arcusto.phone_number, '[^0-9]', '')
                -- if 10 or fewer digits, keep them all
            end                                                     as phone
            ,case
                -- If the number does not start with "00" or "+", treat extra digits as an extension
                when not regexp_like(arcusto.phone_number, '^(\\+|00)')
                    and length(regexp_replace(arcusto.phone_number, '[^0-9]', '')) > 10
                    then
                    substr(
                        case
                            when substr(regexp_replace(arcusto.phone_number, '[^0-9]', ''), 1, 1) = '1'
                            then substr(regexp_replace(arcusto.phone_number, '[^0-9]', ''), 2)
                            else regexp_replace(arcusto.phone_number, '[^0-9]', '')
                        end, 11
                    )
                else
                    null
            end                                                          as extension
            ,'US Dollar'                                                 as primary_currency
            ,null                                                        as price_level
            ,terms.description                                           as terms
            ,arcusto.credit_limit                                        as credit_limit
            ,case
                when arcusto.status_id = 'Cr Hold'
                    then 'On'
                else 'Auto'
            end                                                          as hold
            ,case
                when arcusto.status_id = 'Cr Hold'
                    then to_date(arcusto.status_date)
                else null
            end                                                          as on_hold_since
            ,null                                                        as tag_reg_number
            ,case
                when arcusto.cuser1 in ('DEALER')
                    then null
                else v.industry
            end                                                          as market_industry
            ,arcusto.cuser1                                              as channel_segment
            ,case
                -- Extract email from '<>' if it contains '@' and does not contain 'www.'
                when position('@' in lower(arcusto.prime_contact_email)) > 0
                    and position('www.' in lower(arcusto.prime_contact_email)) = 0
                    and position('<' in lower(arcusto.prime_contact_email)) > 0
                    and position('>' in lower(arcusto.prime_contact_email)) > 0
                then
                    substring(
                        lower(arcusto.prime_contact_email),
                        position('<' in lower(arcusto.prime_contact_email)) + 1,
                        position('>' in lower(arcusto.prime_contact_email)) - position(
                            '<' in lower(arcusto.prime_contact_email)) - 1
                    )

                -- Set to NULL if it contains 'www.' or is not a valid email
                when position('@' in lower(arcusto.prime_contact_email)) = 0
                    or position('www.' in lower(arcusto.prime_contact_email)) > 0
                then
                    null

                -- Otherwise, keep the email as is
                else lower(arcusto.prime_contact_email)
            end as customer_sales_order_emails
            ,case
                -- Extract email from '<>' if it contains '@' and does not contain 'www.'
                when position('@' in lower(arcusto.prime_contact_email)) > 0
                    and position('www.' in lower(arcusto.prime_contact_email)) = 0
                    and position('<' in lower(arcusto.prime_contact_email)) > 0
                    and position('>' in lower(arcusto.prime_contact_email)) > 0
                then
                    substring(
                        lower(arcusto.prime_contact_email),
                        position('<' in lower(arcusto.prime_contact_email)) + 1,
                        position('>' in lower(arcusto.prime_contact_email)) - position(
                            '<' in lower(arcusto.prime_contact_email)) - 1
                    )

                -- Set to NULL if it contains 'www.' or is not a valid email
                when position('@' in lower(arcusto.prime_contact_email)) = 0
                    or position('www.' in lower(arcusto.prime_contact_email)) > 0
                then
                    null

                -- Otherwise, keep the email as is
                else lower(arcusto.prime_contact_email)
            end as customer_invoice_emails
            ,case
                when arcusto.tax_exempt_num is not null
                    then 'FALSE'
                else 'TRUE'
            end as taxable
            ,row_number() over (
                partition by unique_customers.arcusto_id
                order by
                    case when v.industry is not null then 1 else 2 end
                -- prioritize non-null market_industry
            ) as row_num

        from unique_customers
        left outer join fivetran_raw.oracle_iqms.arcusto        arcusto
            on unique_customers.arcusto_id = arcusto.id
        left outer join fivetran_raw.oracle_iqms.terms          terms
            on arcusto.terms_id = terms.id
        left outer join unique_industries
            on unique_customers.arcusto_id = unique_industries.arcusto_id
        left outer join crm_opportunity c
            on unique_industries.arcusto_id = c.principle_source_id
        left outer join iqms.public.v_ud_crm_opportunity v
            on c.id = v.parent_id
        where unique_customers.arcusto_id is not null
    ) ranked_final
    where row_num = 1 -- Keep only the top-ranked row for each arcusto_id
    order by external_id
)
////////for Customer Tab////////////
-- select * from final order by id

,unique_bill_to_addresses as (

    select distinct final.external_id, orders.bill_to_id, count(orders.id) as total

    from final
    left outer join fivetran_raw.oracle_iqms.orders
        on final.external_id = orders.arcusto_id

    group by final.external_id, orders.bill_to_id

    union

    select distinct final.external_id, hist_orders.bill_to_id, count(hist_orders.id) as total

    from final
    left outer join fivetran_raw.oracle_iqms.hist_orders hist_orders
        on final.external_id = hist_orders.arcusto_id

    group by final.external_id, hist_orders.bill_to_id

    union

    select distinct final.external_id, c.bill_to_id, count(c.id) as total

    from final
    left outer join fivetran_raw.oracle_iqms.crm_activity c
        on final.external_id = c.principle_source_id

    where principle_source = 'ARCUSTO' and type = '4'

    group by final.external_id, c.bill_to_id

    union

    select distinct final.external_id, i.bill_to_id, count(i.id) as total

    from final
    left outer join fivetran_raw.oracle_iqms.arinvoice i
        on final.external_id = i.arcusto_id

    group by final.external_id, i.bill_to_id

)

,ranked_bill_to as (

    select distinct
        bill_to_id,
        external_id,
        sum(total) as order_count,
        row_number() over (
            partition by external_id
            order by sum(total) desc
        ) as rn

    from unique_bill_to_addresses

    where bill_to_id is not null

    group by bill_to_id, external_id

)

,bill_to_final as (

    select distinct
        rs.external_id                                          as customer_external_id
        ,'B' || rs.bill_to_id                                   as address_external_id
        ,bill_to.addr1 || ', ' || bill_to.city || ', ' || bill_to.state || ', ' || (case
            when initcap(lower(bill_to.country)) in ('United States Of America', '282')
                then 'United States'
            else initcap(lower(bill_to.country))
        end)                                                    as address_label
        ,bill_to.attn                                           as address_1_attention
        ,case
        -- If the number starts with "00" or "+", keep the full cleaned string
            when regexp_like(bill_to.phone_number, '^(\\+|00)') then
            regexp_replace(bill_to.phone_number, '[^0-9]', '')
            -- remove non-digit characters, keep full number
        -- For standard North American numbers, keep only the first 10 digits
            when length(regexp_replace(bill_to.phone_number, '[^0-9]', '')) > 10 then
            substr(
                case
                    when substr(regexp_replace(bill_to.phone_number, '[^0-9]', ''), 1, 1) = '1'
                    then substr(regexp_replace(bill_to.phone_number, '[^0-9]', ''), 2)
                    else regexp_replace(bill_to.phone_number, '[^0-9]', '')
                end, 1, 10
            )
            else
            regexp_replace(bill_to.phone_number, '[^0-9]', '')
            -- if 10 or fewer digits, keep them all
         end                                                     as address_1_phone
        ,case
        -- If the number does not start with "00" or "+", treat extra digits as an extension
            when not regexp_like(bill_to.phone_number, '^(\\+|00)')
                and length(regexp_replace(bill_to.phone_number, '[^0-9]', '')) > 10
                then
                substr(
                    case
                        when substr(regexp_replace(bill_to.phone_number, '[^0-9]', ''), 1, 1) = '1'
                        then substr(regexp_replace(bill_to.phone_number, '[^0-9]', ''), 2)
                        else regexp_replace(bill_to.phone_number, '[^0-9]', '')
                    end, 11
                )
            else
            null
        end                                                     as address_1_phone_extension
        ,bill_to.addr1                                          as address_1_address_1
        ,bill_to.addr2                                          as address_1_address_2
        ,bill_to.city                                           as address_1_city
        ,bill_to.state                                          as address_1_state
        ,bill_to.zip                                            as address_1_zip
        ,case
            when initcap(lower(bill_to.country)) in ('United States Of America', '282')
                then 'United States'
            else initcap(lower(bill_to.country))
        end                                                     as address_1_country
        ,case
            when rs.rn = 1 then 'TRUE'
            else 'FALSE'
        end                                                     as addrees_1_default_billing
        ,'FALSE'                                                as addrees_1_default_shipping

    from ranked_bill_to rs
    left outer join fivetran_raw.oracle_iqms.bill_to                        bill_to
        on rs.bill_to_id = bill_to.id
    left outer join fivetran_raw.oracle_iqms.arcusto                        arcusto
        on rs.external_id = arcusto.id


    order by customer_external_id

)

////////for Customer Address Tab - Bill To///
-- select * from bill_to_final order by customer_external_id

,unique_ship_to_addresses as (

    select distinct final.external_id, orders.ship_to_id, count(orders.id) as total

    from final
    left outer join fivetran_raw.oracle_iqms.orders
        on final.external_id = orders.arcusto_id

    group by final.external_id, orders.ship_to_id

    union

    select distinct final.external_id, hist_orders.ship_to_id, count(hist_orders.id) as total

    from final
    left outer join fivetran_raw.oracle_iqms.hist_orders hist_orders
        on final.external_id = hist_orders.arcusto_id

    group by final.external_id, hist_orders.ship_to_id

    union

    select distinct final.external_id, c.ship_to_id, count(c.id) as total

    from final
    left outer join fivetran_raw.oracle_iqms.crm_activity c
        on final.external_id = c.principle_source_id

    where principle_source = 'ARCUSTO' and type = '4'

    group by final.external_id, c.ship_to_id

)

,ranked_ship_to as (

    select distinct
        ship_to_id,
        external_id,
        sum(total) as order_count,
        row_number() over (
            partition by external_id
            order by sum(total) desc
        ) as rn

    from unique_ship_to_addresses

    where ship_to_id is not null

    group by ship_to_id, external_id

)

,ship_to_final as (

    select distinct
        rs.external_id          as customer_external_id
        ,'S' || rs.ship_to_id   as address_external_id
        ,ship_to.addr1 || ', ' || ship_to.city || ', ' || ship_to.state || ', ' || (case
            when initcap(lower(ship_to.country)) in ('United States Of America', '282')
                then 'United States'
            else initcap(lower(ship_to.country))
        end)                    as address_label
        ,ship_to.attn           as address_1_attention
        ,case
        -- If the number starts with "00" or "+", keep the full cleaned string
            when regexp_like(ship_to.phone_number, '^(\\+|00)') then
            regexp_replace(ship_to.phone_number, '[^0-9]', '') -- remove non-digit characters, keep full number
        -- For standard North American numbers, keep only the first 10 digits
            when length(regexp_replace(ship_to.phone_number, '[^0-9]', '')) > 10 then
            substr(
                case
                    when substr(regexp_replace(ship_to.phone_number, '[^0-9]', ''), 1, 1) = '1'
                    then substr(regexp_replace(ship_to.phone_number, '[^0-9]', ''), 2)
                    else regexp_replace(ship_to.phone_number, '[^0-9]', '')
                end, 1, 10
            )
            else
            regexp_replace(ship_to.phone_number, '[^0-9]', '') -- if 10 or fewer digits, keep them all
         end                    as address_1_phone
        ,case
        -- If the number does not start with "00" or "+", treat extra digits as an extension
            when not regexp_like(ship_to.phone_number, '^(\\+|00)')
                and length(regexp_replace(ship_to.phone_number, '[^0-9]', '')) > 10
                then
                substr(
                    case
                        when substr(regexp_replace(ship_to.phone_number, '[^0-9]', ''), 1, 1) = '1'
                        then substr(regexp_replace(ship_to.phone_number, '[^0-9]', ''), 2)
                        else regexp_replace(ship_to.phone_number, '[^0-9]', '')
                    end, 11
                )
            else
            null
        end                     as address_1_phone_extension
        ,ship_to.addr1          as address_1_address_1
        ,ship_to.addr2          as address_1_address_2
        ,ship_to.city           as address_1_city
        ,ship_to.state          as address_1_state
        ,ship_to.zip            as address_1_zip
        ,case
            when initcap(lower(ship_to.country)) in ('United States Of America', '282')
                then 'United States'
            else initcap(lower(ship_to.country))
        end                     as address_1_country
        ,case
            when rs.rn = 1 then 'TRUE'
            else 'FALSE'
        end                     as addrees_1_default_billing
        ,'FALSE'                as addrees_1_default_shipping

    from ranked_ship_to rs
    left outer join fivetran_raw.oracle_iqms.ship_to                        ship_to
        on rs.ship_to_id = ship_to.id
    left outer join fivetran_raw.oracle_iqms.arcusto                        arcusto
        on rs.external_id = arcusto.id


    order by customer_external_id

)

////////for Customer Address Tab - Ship To///
-- select * from ship_to_final order by customer_external_id

,customers_addresses_final as (

    select distinct
        rs.arcusto_id           as customer_external_id
        ,'C' || rs.arcusto_id   as address_external_id
        ,arcusto.addr1 || ', ' || arcusto.city || ', ' || arcusto.state || ', ' || (case
            when initcap(lower(arcusto.country)) in ('United States Of America', '282')
                then 'United States'
            else initcap(lower(arcusto.country))
        end)                    as address_label
        ,arcusto.company        as address_1_attention
        ,case
        -- If the number starts with "00" or "+", keep the full cleaned string
            when regexp_like(arcusto.phone_number, '^(\\+|00)') then
            regexp_replace(arcusto.phone_number, '[^0-9]', '') -- remove non-digit characters, keep full number
        -- For standard North American numbers, keep only the first 10 digits
            when length(regexp_replace(arcusto.phone_number, '[^0-9]', '')) > 10 then
            substr(
                case
                    when substr(regexp_replace(arcusto.phone_number, '[^0-9]', ''), 1, 1) = '1'
                    then substr(regexp_replace(arcusto.phone_number, '[^0-9]', ''), 2)
                    else regexp_replace(arcusto.phone_number, '[^0-9]', '')
                end, 1, 10
            )
            else
            regexp_replace(arcusto.phone_number, '[^0-9]', '') -- if 10 or fewer digits, keep them all
         end                                                     as address_1_phone
        ,case
        -- If the number does not start with "00" or "+", treat extra digits as an extension
            when not regexp_like(arcusto.phone_number, '^(\\+|00)')
                and length(regexp_replace(arcusto.phone_number, '[^0-9]', '')) > 10
                then
                substr(
                    case
                        when substr(regexp_replace(arcusto.phone_number, '[^0-9]', ''), 1, 1) = '1'
                        then substr(regexp_replace(arcusto.phone_number, '[^0-9]', ''), 2)
                        else regexp_replace(arcusto.phone_number, '[^0-9]', '')
                    end, 11
                )
            else
            null
        end                     as address_1_phone_extension
        ,arcusto.addr1          as address_1_address_1
        ,arcusto.addr2          as address_1_address_2
        ,arcusto.city           as address_1_city
        ,arcusto.state          as address_1_state
        ,arcusto.zip            as address_1_zip
        ,case
            when initcap(lower(arcusto.country)) in ('United States Of America', '282')
                then 'United States'
            else initcap(lower(arcusto.country))
        end                     as address_1_country
        ,'FALSE'                as addrees_1_default_billing
        ,'FALSE'                as addrees_1_default_shipping

    from unique_customers rs
    left outer join fivetran_raw.oracle_iqms.arcusto                        arcusto
        on rs.arcusto_id = arcusto.id


    order by customer_external_id

)

////////for Customer Address Tab - Customers///
select * from customers_addresses_final order by customer_external_id;