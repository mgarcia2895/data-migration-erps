-- CTE to extract purchase order details with relevant columns and remove duplicates
with purchase_orders as (

    select distinct
        id as po_id
        ,pono
        ,ship_to_id
        ,vendor_id
        ,remit_to_id

    from fivetran_raw.oracle_iqms.po
    -- Filter to specific vendor IDs (commented out)
    --where vendor_id in ('62377', '63049', '62705', '64203', '64128')

)

-- Main query to fetch and format vendor information
,all_vendors as (

    select
        vendor.id                                                   as external_id
        ,cast(initcap(lower(vendor.vendorno)) as string)            as vendor_id
        ,initcap(lower(vendor.company))                             as vendor_name
        ,null                                                       as Primary_Subsidiary_External_ID
        ,case
            when vendor.company = dim_employees.employee_full_name
                then 'TRUE'
            else 'FALSE'
        end                                                         as Individual
        ,case
            when vendor.cuser2 = 'Strategic Buyer'
                then 'Strategic'
            when vendor.cuser2 = 'Tactical Buyer'
                then 'Tactical'
            else null
        end                                                         as category
        ,vendor.e_mail_addr                                         as email
        ,case
            -- If the number starts with "00" or "+", keep the full cleaned string
            when regexp_like(vendor.phone_number, '^(\\+|00)') then
                regexp_replace(vendor.phone_number, '[^0-9]', '')
                    -- remove non-digit characters, keep full number
            -- For standard North American numbers, keep only the first 10 digits
            when length(regexp_replace(vendor.phone_number, '[^0-9]', '')) > 10 then
                substr(
                    case
                        when substr(regexp_replace(vendor.phone_number, '[^0-9]', ''), 1, 1) = '1'
                        then substr(regexp_replace(vendor.phone_number, '[^0-9]', ''), 2)
                        else regexp_replace(vendor.phone_number, '[^0-9]', '')
                    end, 1, 10
                )
            else
                regexp_replace(vendor.phone_number, '[^0-9]', '')
                -- if 10 or fewer digits, keep them all
        end                                                          as phone

        -- Extract the extension if the phone number exceeds 10 digits
        ,case
            -- If the number does not start with "00" or "+", treat extra digits as an extension
            when not regexp_like(vendor.phone_number, '^(\\+|00)')
                and length(regexp_replace(vendor.phone_number, '[^0-9]', '')) > 10 then
                substr(
                    case
                        when substr(regexp_replace(vendor.phone_number, '[^0-9]', ''), 1, 1) = '1'
                        then substr(regexp_replace(vendor.phone_number, '[^0-9]', ''), 2)
                        else regexp_replace(vendor.phone_number, '[^0-9]', '')
                    end, 11
                )
            else
                null
        end                                                          as extension
        ,vendor.web_site_url                                         as web_address
        ,case
            -- If the number starts with "00" or "+", keep the full cleaned string
            when regexp_like(vendor.fax_number, '^(\\+|00)') then
                regexp_replace(vendor.fax_number, '[^0-9]', '')
                    -- remove non-digit characters, keep full number
            -- For standard North American numbers, keep only the first 10 digits
            when length(regexp_replace(vendor.fax_number, '[^0-9]', '')) > 10 then
                substr(
                    case
                        when substr(regexp_replace(vendor.fax_number, '[^0-9]', ''), 1, 1) = '1'
                        then substr(regexp_replace(vendor.fax_number, '[^0-9]', ''), 2)
                        else regexp_replace(vendor.fax_number, '[^0-9]', '')
                    end, 1, 10
                )
            else
                regexp_replace(vendor.fax_number, '[^0-9]', '')
                -- if 10 or fewer digits, keep them all
        end                                                          as fax
        ,null                                                        as address_1_external_id
        ,null                                                        as address_1_adressee
        ,case
            -- If the number starts with "00" or "+", keep the full cleaned string
            when regexp_like(vendor.phone_number, '^(\\+|00)') then
                regexp_replace(vendor.phone_number, '[^0-9]', '')
                    -- remove non-digit characters, keep full number
            -- For standard North American numbers, keep only the first 10 digits
            when length(regexp_replace(vendor.phone_number, '[^0-9]', '')) > 10 then
                substr(
                    case
                        when substr(regexp_replace(vendor.phone_number, '[^0-9]', ''), 1, 1) = '1'
                        then substr(regexp_replace(vendor.phone_number, '[^0-9]', ''), 2)
                        else regexp_replace(vendor.phone_number, '[^0-9]', '')
                    end, 1, 10
                )
            else
                regexp_replace(vendor.phone_number, '[^0-9]', '')
                    -- if 10 or fewer digits, keep them all
        end                                                          as address_1_phone

        -- Extract the extension if the phone number exceeds 10 digits
        ,case
            -- If the number does not start with "00" or "+", treat extra digits as an extension
            when not regexp_like(vendor.phone_number, '^(\\+|00)')
                and length(regexp_replace(vendor.phone_number, '[^0-9]', '')) > 10 then
                substr(
                    case
                        when substr(regexp_replace(vendor.phone_number, '[^0-9]', ''), 1, 1) = '1'
                        then substr(regexp_replace(vendor.phone_number, '[^0-9]', ''), 2)
                        else regexp_replace(vendor.phone_number, '[^0-9]', '')
                    end, 11
                )
            else
                null
        end                                                          as address_1_phone_extension
        ,initcap(lower(vendor.addr1))                                as address_1_address_1
        ,null                                                        as address_1_address_2
        ,initcap(lower(vendor.city))                                 as address_1_city
        ,initcap(lower(states.name))                                 as address_1_state
        ,initcap(lower(vendor.zip))                                  as address_1_zip
        ,case
            when initcap(lower(vendor.country)) in ('United States Of America', '282')
                then 'United States'
            else initcap(lower(vendor.country))
        end                                                          as address_1_country
        ,null                                                        as address_1_default_shipping
        ,null                                                        as address_1_default_billing

        --- Bill-to address information
            -- ,remit_to.id                                 as bill_to_id

            -- Formatting and validating phone numbers
        ,null                                                        as address_2_external_id
        ,initcap(lower(remit_to.attn))                               as address_2_adressee
        ,case
            -- If the number starts with "00" or "+", keep the full cleaned string
            when regexp_like(remit_to.phone_number, '^(\\+|00)') then
                regexp_replace(remit_to.phone_number, '[^0-9]', '')
                    -- remove non-digit characters, keep full number
            -- For standard North American numbers, keep only the first 10 digits
            when length(regexp_replace(remit_to.phone_number, '[^0-9]', '')) > 10 then
                substr(
                    case
                        when substr(regexp_replace(remit_to.phone_number, '[^0-9]', ''), 1, 1) = '1'
                        then substr(regexp_replace(remit_to.phone_number, '[^0-9]', ''), 2)
                        else regexp_replace(remit_to.phone_number, '[^0-9]', '')
                    end, 1, 10
                )
            else
                regexp_replace(remit_to.phone_number, '[^0-9]', '')
                    -- if 10 or fewer digits, keep them all
        end                                                          as address_2_phone

        -- Extract the extension if the phone number exceeds 10 digits
        ,case
            -- If the number does not start with "00" or "+", treat extra digits as an extension
            when not regexp_like(remit_to.phone_number, '^(\\+|00)')
                and length(regexp_replace(remit_to.phone_number, '[^0-9]', '')) > 10 then
                substr(
                    case
                        when substr(regexp_replace(remit_to.phone_number, '[^0-9]', ''), 1, 1) = '1'
                        then substr(regexp_replace(remit_to.phone_number, '[^0-9]', ''), 2)
                        else regexp_replace(remit_to.phone_number, '[^0-9]', '')
                    end, 11
                )
            else
                null
        end                                                          as address_2_phone_extension
        ,initcap(lower(remit_to.addr1))                              as address_2_address_1
        ,initcap(lower(remit_to.addr2))                              as address_2_address_2
        ,initcap(lower(remit_to.city))                               as address_2_city
        ,initcap(lower(states2.name))                                as address_2_state
        ,remit_to.zip                                                as address_2_zip
        ,case
            when initcap(lower(remit_to.country)) in ('United States Of America', '282')
                then 'United States'
            else initcap(lower(remit_to.country))
        end                                                          as address_2_country
        ,null                                                        as Address_2_Default_Shipping
        ,null                                                        as Address_2_Default_Billing
        ,'US Dollar'                                                 as currency
        ,initcap(lower(terms.description))                           as terms
        ,null                                                        as incoterm
        ,vendor.fed_tax_id                                           as tax_id
        ,nvl(vendor.include_in_1099, 'N')                            as include_in_1099
        ,glacct.acct                                                 as account
        ,null                                                        as inactive


    -- Joining tables to include purchase order, remit-to, terms, and state information

    from fivetran_raw.oracle_iqms.vendor vendor
    left outer join purchase_orders
        on vendor.id = purchase_orders.vendor_id
    left outer join fivetran_raw.oracle_iqms.remit_to remit_to
        on purchase_orders.remit_to_id = remit_to.id
    left outer join fivetran_raw.oracle_iqms.terms terms
        on vendor.terms_id = terms.id
    left outer join miguelg.public.states states
        on vendor.state = states.state_code
    left outer join miguelg.public.states states2
        on remit_to.state = states2.state_code
    left outer join analytics.dbt_core.dim_employees dim_employees
        on vendor.company = dim_employees.employee_full_name
    left outer join fivetran_raw.oracle_iqms.glacct glacct
        on vendor.glacct_id_exp = glacct.id

    group by
        vendor.id
        ,vendor.vendorno
        ,vendor.company
        ,dim_employees.employee_full_name
        ,vendor.cuser2
        ,vendor.e_mail_addr
        ,vendor.phone_number
        ,vendor.web_site_url
        ,vendor.fax_number
        ,vendor.addr1
        ,vendor.city
        ,states.name
        ,vendor.zip
        ,vendor.country
        ,remit_to.id
        ,remit_to.attn
        ,remit_to.phone_number
        ,remit_to.addr1
        ,remit_to.addr2
        ,remit_to.addr3
        ,remit_to.city
        ,states2.name
        ,remit_to.zip
        ,remit_to.country
        ,terms.description
        ,vendor.fed_tax_id
        ,vendor.include_in_1099
        ,glacct.acct

    --having count(purchase_orders.po_id) > 0


    order by vendor.id desc, remit_to.attn desc

)

,vendors_cleaned as (

    select * from miguelg.public.data_migration_vendor_full

)

select
    all_vendors.external_id
    ,case
        when left(all_vendors.vendor_id, 1) = '5'
            then cast(all_vendors.vendor_id as string)
        else cast(lpad(all_vendors.vendor_id, 10, '0') as string)
    end as vendor_id
    ,all_vendors.vendor_name
    ,all_vendors.primary_subsidiary_external_id
    ,all_vendors.individual
    ,all_vendors.category
    ,all_vendors.email
    ,vendors_cleaned.email
    ,all_vendors.phone
    ,all_vendors.extension
    ,all_vendors.web_address
    ,vendors_cleaned.web_address
    ,vendors_cleaned.fax
    ,vendors_cleaned.address_1_external_id
    ,all_vendors.address_1_adressee
    ,all_vendors.address_1_phone
    ,all_vendors.address_1_phone_extension
    ,all_vendors.address_1_address_1
    ,all_vendors.address_1_address_2
    ,all_vendors.address_1_city
    ,vendors_cleaned.address_1_city
    ,all_vendors.address_1_state
    ,vendors_cleaned.address_1_state
    ,all_vendors.address_1_zip
    ,vendors_cleaned.address_1_zip
    ,all_vendors.address_1_country
    ,vendors_cleaned.address_1_country
    ,'True' as address_1_default_shipping
    ,case
        when all_vendors.address_2_adressee is null
            then true
        else false
    end as address_1_default_billing
    ,vendors_cleaned.address_2_external_id
    ,all_vendors.address_2_adressee
    ,all_vendors.address_2_phone
    ,all_vendors.address_2_phone_extension
    ,all_vendors.address_2_address_1
    ,vendors_cleaned.address_2_address_1
    ,all_vendors.address_2_address_2
    ,vendors_cleaned.address_2_address_2
    ,all_vendors.address_2_city
    ,vendors_cleaned.address_2_city
    ,all_vendors.address_2_state
    ,vendors_cleaned.address_2_state
    ,all_vendors.address_2_zip
    ,vendors_cleaned.address_2_zip
    ,all_vendors.address_2_country
    ,vendors_cleaned.address_2_country
    ,'False' as address_2_default_shipping
    ,case
        when all_vendors.address_2_adressee is not null
            then true
        else false
    end as address_2_default_billing
    ,all_vendors.currency
    ,all_vendors.terms
    ,all_vendors.incoterm
    ,case
        when all_vendors.include_in_1099 = 'N'
            then false
        else true
    end as INCLUDE_IN_1099
    ,all_vendors.tax_id
    ,all_vendors.account
    ,all_vendors.inactive


from vendors_cleaned
left outer join all_vendors
    on vendors_cleaned.external_id = all_vendors.external_id

order by external_id asc
;