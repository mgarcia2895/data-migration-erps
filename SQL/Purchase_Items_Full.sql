--Purchase Items data aligned for data migration.
-- Selected columns are made to populate item template.
with
-- this is for bringing in item classes assigned in netsuite
netsuite_item_class as (

    select * from miguelg.public.netsuite_item_class

)

-- this is yielding units of measurement for item
-- to determine purchase and sale unit for an item
,po_detail_comb as (

    select
        po.po_date
        ,po_detail.arinvt_id
        ,po_detail.unit

    from fivetran_raw.oracle_iqms.po_detail po_detail
    left outer join fivetran_raw.oracle_iqms.po po
        on po_detail.po_id = po.id

    union

    select
        po_hist.po_date
        ,po_detail_hist.arinvt_id
        ,po_detail_hist.unit

    from fivetran_raw.oracle_iqms.po_detail_hist
    left outer join fivetran_raw.oracle_iqms.po_hist po_hist
        on po_detail_hist.po_id = po_hist.id

)

-- this helps determine the most recent po date for an item
-- to yield current purchase/sale uom
,po_dates_assign as (

    select
        po_date
        ,arinvt_id
        ,unit
        ,row_number() over (partition by arinvt_id order by po_date desc) as rn

    from po_detail_comb


)

-- this yields the purchase/selling uom
-- keeps the grain to one row per inventory id
,max_item_po_date as (

    select
        po_date
        ,arinvt_id
        ,unit

    from po_dates_assign
    where rn = 1

)

-- this helps to group the models/product groups of items
-- to the designated class in netsuite.
-- will be joined with netsuite_item_class cte on netsuite_class
,item_class as (

    select
        dim_inventory.inventory_id
        ,case
            when dim_inventory.product_group in (
                                                 'Pallet Jacks - Parts',
                                                 'Electric Vehicles - Parts',
                                                 'Trailers - Parts'
                                                )
                then 'Parts'
            when dim_inventory.product_model in (
                                                 'BC-520 NXG Series', 'BC-520 Series',
                                                 'BC-620 NXG Series', 'BC-620 Series',
                                                 'BCG-6200 Series', 'BCT-9000 NXG Series',
                                                 'BCT-9000 Series', 'BC-625 Series'
                                                )
                then 'Burden Carrier'
            when dim_inventory.product_model in (
                                                 'Mall Train', 'SC-775 NXG Series',
                                                 'SC-775 Series', 'SC-850 NXG Series',
                                                 'SC-850 Series', 'SC-750 Series',
                                                 'SCT-8500 NXG Series'
                                                )
                then 'Stock Chaser'
            when dim_inventory.product_model in (
                                                 'PC-300 NXG Series', 'PC-300 Series',
                                                 'PC-320 NXG Series', 'PC-320 Series',
                                                 'PC-325 NXG Series', 'PC-325 Series',
                                                 'PC-640 NXG Series','PC-640 Series',
                                                 'PCT-3500 NXG Series', 'PCT-3500 Series',
                                                 'PC-310 NXG Series', 'Sprinter NXG Series'
                                                )
                then 'Personnel Carrier'
            when dim_inventory.product_model in (
                                                 'PCT-3500 NXG Series', 'PCT-3500 Series',
                                                 'PMT-4500 NXG Series', 'PMT-4500 Series',
                                                 'PMT-5000 NXG Series', 'PMT-5000 Series',
                                                 'PMT-6000 Series'
                                                )
                then 'Tugger'
            when dim_inventory.product_model in ('Double Fork Pallet Jack')
                then 'PJ Double Fork'
            when dim_inventory.product_model in ('High Capacity Pallet Jack')
                then 'PJ High Capacity'
            when dim_inventory.product_model in ('Reel Truck Pallet Jack', 'Towable Pallet Jack')
                then 'PJ Roll/Reel Truck'
            when dim_inventory.product_model in ('Single Fork Pallet Jack', 'Narrow Fork Pallet Jack')
                then 'PJ Single Fork'
            when dim_inventory.product_model in ('Skid Truck Pallet Jack')
                then 'PJ Skid Truck'
            when dim_inventory.product_model in ('Shipboard Pallet Jack')
                then 'Navy Shipboard'
            when dim_inventory.product_model in (
                                                 'DC-1000 Towable Cart', 'DC-2000 Towable Cart',
                                                 'DC-3/4000 Towable Cart'
                                                )
                then 'Delivery Cart'
            when dim_inventory.product_model in ('FedEx Towable Cart', 'Modular Cart')
                then 'Cargo Cart'
            when dim_inventory.product_model in ('TOP-1500 Towable Cart')
                then 'TOP Cart'
            when dim_inventory.product_model in ('DP-3/4000 Towable Cart')
                then 'Double Pallet'
            when dim_inventory.product_model in ('Not a Finished Good', 'Unknown Model')
                then null
            when dim_inventory.product_model in (
                                                 'SP-1000 Towable Cart', 'SP-1500 Towable Cart',
                                                 'SP-2500 Towable Cart', 'SP-3/4000 Towable Cart'
                                                )
                then 'Single Pallet'
            when dim_inventory.product_model in ('Coke Pallet Jack')
                then 'Coke Pallet Jack'
            else null
        end as netsuite_class

    from analytics.dbt_core.dim_inventory   dim_inventory

)

-- this helps yields the most recent price break quans and prices per vendor
,vendor_price as (

    select
        max(id) as vendor_price_break_id
        ,arinvt_vendors_id
        ,quan
        ,qprice

    from fivetran_raw.oracle_iqms.arinvt_vendors_breaks

    where deactive_date is null

    group by arinvt_vendors_id, quan, qprice

)

-- a parameter to help flush out any unused/limited transacted items

,arinvt_to_exclude as (
    -- Identify items to exclude based on two conditions:
    -- 1. Items with 1 or fewer transactions before 2024.
    -- 2. Items with no transactions since 2020 and on-hand quantity = 0.
    select distinct
        arinvt.id as arinvt_id
        ,count(translog.id) as transaction_count
        ,max(translog.trans_date) as last_transaction_date
        -- Get the creation date from the first 'NEW ITEM' transaction
        ,min(case when translog.trans_type = 'NEW ITEM' then translog.trans_date end) as creation_date
        ,arinvt.onhand

    from fivetran_raw.oracle_iqms.translog translog
    left outer join fivetran_raw.oracle_iqms.arinvt arinvt
        on translog.arinvt_id = arinvt.id

    group by arinvt.id, arinvt.onhand
    having
        (
            -- Condition 1: Created before 2024 and has 1 or fewer transactions
            creation_date < TO_DATE('2024-01-01', 'YYYY-MM-DD')
            and transaction_count <= 1
        )
        or
        (
            -- Condition 2: No transactions since set date and on-hand quantity = 0
            last_transaction_date < TO_DATE('2021-01-01', 'YYYY-MM-DD')
            and nvl(onhand, 0) = 0
        )

)

-- select
--     d.item_number,
--     a.arinvt_id,
--     a.transaction_count,
--     a.creation_date,
--     a.last_transaction_date,
--     a.onhand
-- from arinvt_to_exclude a
-- left join analytics.dbt_core.dim_inventory d
--     on a.arinvt_id = d.inventory_id
-- -- where a.transaction_count > 1
-- -- where a.transaction_count = 1
-- order by a.last_transaction_date desc;

/// main query ///
,results as (

    select
        dim_inventory.inventory_id        as external_id
        ,dim_inventory.item_number        as item_name_number
        ,dim_inventory.item_description   as display_name
        ,dim_inventory.item_description   as purchase_description
        ,dim_inventory.item_description   as sales_description
        ,'Wesley International LLC'       as subsidiary
        ,'FALSE'                          as include_children
        ,case
            when dim_inventory.unit_of_measure in ('EACH', 'PAIR')
                then '1'
            when dim_inventory.unit_of_measure in ('YARD', 'IN', 'FT', 'ROLL')
                then '3'
            when dim_inventory.unit_of_measure in ('GAL')
                then '4'
            else null

        end                               as units_type
        ,case
            when arinvt.unit in ('YARD')
                then 'YD'
            when arinvt.unit in ('EACH')
                then 'EA'
            when arinvt.unit in ('PAIR')
                then 'PR'
            when arinvt.unit in ('GAL')
                then 'Gallon'
            else arinvt.unit
        end                               as primary_stock_unit
        ,case
            when max_item_po_date.unit in ('YARD')
                then 'YD'
            when max_item_po_date.unit in ('EACH', 'UNIT')
                then 'EA'
            when max_item_po_date.unit in ('PAIR')
                then 'PR'
            when max_item_po_date.unit in ('GAL')
                then 'Gallon'
            when max_item_po_date.unit in ('ROLL')
                then 'IN'
            else (case
                    when arinvt.unit in ('YARD')
                        then 'YD'
                    when arinvt.unit in ('EACH')
                        then 'EA'
                    when arinvt.unit in ('PAIR')
                        then 'PR'
                    when arinvt.unit in ('GAL')
                        then 'Gallon'
                    else arinvt.unit
                end)
        end                               as primary_purchase_unit
        ,case
            when max_item_po_date.unit in ('YARD')
                then 'YD'
            when max_item_po_date.unit in ('EACH', 'UNIT')
                then 'EA'
            when max_item_po_date.unit in ('PAIR')
                then 'PR'
            when max_item_po_date.unit in ('GAL')
                then 'Gallon'
            when max_item_po_date.unit in ('ROLL')
                then 'IN'
            else (case
                    when arinvt.unit in ('YARD')
                        then 'YD'
                    when arinvt.unit in ('EACH')
                        then 'EA'
                    when arinvt.unit in ('PAIR')
                        then 'PR'
                    when arinvt.unit in ('GAL')
                        then 'Gallon'
                    else arinvt.unit
                end)
        end                               as primary_sale_unit
        ,case
            when arinvt.unit in ('YARD')
                then 'YD'
            when arinvt.unit in ('EACH')
                then 'EA'
            when arinvt.unit in ('PAIR')
                then 'PR'
            when arinvt.unit in ('GAL')
                then 'Gallon'
            else arinvt.unit
        end                               as primary_consumption_unit
        ,netsuite_item_class.netsuite_id  as class
        ,'Raw Material'                   as cost_category
        ,null                             as planning_item_category
        ,'TRUE'                           as Use_Bins
        ,'Material Requirements Planning' as replenishment_method
        ,'TRUE'                           as match_bill_to_receipt
        ,'12010 Inventory : Raw Materials' as asset_account
        ,'40000 Gross Sales'              as income_account
        ,'50000 Cost of Goods Sold'       as cogs_account
        ,'Taxable'                        as tax_schedule

    from analytics.dbt_core.dim_inventory dim_inventory
    left outer join arinvt_to_exclude
        on dim_inventory.inventory_id = arinvt_to_exclude.arinvt_id
    left outer join fivetran_raw.oracle_iqms.arinvt arinvt
        on dim_inventory.inventory_id = arinvt.id
    left outer join item_class                                              item_class
        on dim_inventory.inventory_id = item_class.inventory_id
    left outer join netsuite_item_class                                     netsuite_item_class
        on item_class.netsuite_class = netsuite_item_class.product_group
    left outer join max_item_po_date                                        max_item_po_date
        on dim_inventory.inventory_id = max_item_po_date.arinvt_id

    where arinvt_to_exclude.arinvt_id is null
        and dim_inventory.product_group in (
            'Electric Vehicles - Parts', 'Pallet Jacks - Parts',
            'Trailers - Parts', 'Trailers - F&H',
            'Pallet Jacks - F&H', 'Electric Vehicles - F&H'
            )
        and dim_inventory.default_bom_number is null
        and dim_inventory.inventory_id not in ('206081', '206079', '206100', '206099', '206085')

order by dim_inventory.inventory_id asc

)
////////for Inventory Tab////////////
select * from results;

,location_results as (

        select
            dim_inventory.inventory_id         as external_id
            ,'Wesley International LLC'        as subsidiary
            ,'Georgia HQ'                      as Location_1_Location
            ,null                              as Location_1_Reorder_Point
            ,'Purchase'                        as Location_1_Supply_Type
            ,null                              as Location_1_Preferred_Stock_Level
            ,case
                when dim_inventory.lead_days = 0
                    then null
                else dim_inventory.lead_days
            end                               as Location_1_Purchase_Lead_Time
            ,case
                when arinvt.safety_stock = 0
                    then null
                else arinvt.safety_stock
            end                               as Location_1_Safety_Stock_Level
            ,case
                when dim_inventory.unit_of_measure not in ('EACH')
                    then 'Minimum Order Quantity'
                else 'Lot For Lot'
            end                               as Location_1_Lot_Sizing_Method
            ,dim_inventory.min_order_qty      as min_order_qty
            ,null
            ,null
            ,null
            ,null
            ,null
            ,'120'                            as location_1_count_interval
            ,dim_inventory.standard_cost      as Location_1_Default_Cost
            ,null                              as location_1_classification

        from analytics.dbt_core.dim_inventory dim_inventory
        left outer join arinvt_to_exclude
            on dim_inventory.inventory_id = arinvt_to_exclude.arinvt_id
        left outer join fivetran_raw.oracle_iqms.arinvt arinvt
            on dim_inventory.inventory_id = arinvt.id

        where arinvt_to_exclude.arinvt_id is null
            and dim_inventory.product_group in (
                'Electric Vehicles - Parts', 'Pallet Jacks - Parts',
                'Trailers - Parts', 'Trailers - F&H',
                'Pallet Jacks - F&H', 'Electric Vehicles - F&H'
                )
            and dim_inventory.default_bom_number is null
            and dim_inventory.inventory_id not in ('206081', '206079', '206100', '206099', '206085')

        order by dim_inventory.inventory_id asc

)

////////for Locations Tab////////////
-- select * from location_results;

,vendors_results as (

    select
        dim_inventory.inventory_id                               as external_id
        ,dim_inventory.item_number
        ,arinvt_vendors_breaks.QUAN
        ,arinvt_vendors.vendor_id                                as vendor_external_id
        ,arinvt_vendors.vend_itemno                              as vendor_code
        ,case
            when arinvt_vendors_breaks.quan != 0
                then arinvt_vendors_breaks.qprice/arinvt_vendors_breaks.quan
            else arinvt_vendors_breaks.qprice
        end                                                      as vendor_price
        ,case
            when nvl(arinvt_vendors.is_default, 'N') = 'Y'
                then 'TRUE'
            else 'FALSE'
        end                                                      as Preferred
        ,row_number() over (
            partition by dim_inventory.inventory_id, arinvt_vendors.vendor_id
            order by arinvt_vendors_breaks.QUAN asc
        ) as rn

    from fivetran_raw.oracle_iqms.arinvt_vendors_breaks arinvt_vendors_breaks
    left outer join fivetran_raw.oracle_iqms.arinvt_vendors arinvt_vendors
        on arinvt_vendors_breaks.arinvt_vendors_id = arinvt_vendors.id
    left outer join analytics.dbt_core.dim_inventory dim_inventory
        on arinvt_vendors.arinvt_id = dim_inventory.inventory_id
    left outer join arinvt_to_exclude
        on dim_inventory.inventory_id = arinvt_to_exclude.arinvt_id
    left outer join vendor_price vendor_price
        on arinvt_vendors_breaks.id = vendor_price.vendor_price_break_id

    where arinvt_to_exclude.arinvt_id is null
        and dim_inventory.product_group in (
            'Electric Vehicles - Parts', 'Pallet Jacks - Parts',
            'Trailers - Parts', 'Trailers - F&H', 'Pallet Jacks - F&H',
            'Electric Vehicles - F&H'
            )
        and dim_inventory.default_bom_number is null
        and dim_inventory.inventory_id not in (
            '206081', '206079', '206100', '206099', '206085'
            )
        and vendor_price.arinvt_vendors_id is not null
        and arinvt_vendors_breaks.deactive_date is null
    qualify rn = 1
)

////////for Vendors Tab////////////
select * from vendors_results order by external_id, vendor_external_id;