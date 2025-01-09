-- Assembly Items data aligned for data migration.
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
,items_creation_date as (

    select
        translog.arinvt_id
        ,arinvt.itemno
        ,translog.trans_date as item_creation_date
--         ,translog.trans_type

    from
        fivetran_raw.oracle_iqms.translog translog
    left outer join
        fivetran_raw.oracle_iqms.arinvt arinvt on translog.arinvt_id = arinvt.id
    where
        translog.trans_type = 'NEW ITEM'
        and translog.arinvt_id is not null


)
-- select * from items_creation_date where arinvt_id is not null order by arinvt_id desc, trans_date desc;

,items_last_use_date as (

     select
        translog.arinvt_id
        ,arinvt.itemno
        ,max(translog.trans_date) as recent_trans_date
--         ,translog.trans_type

    from
        fivetran_raw.oracle_iqms.translog translog
    left outer join
        fivetran_raw.oracle_iqms.arinvt arinvt on translog.arinvt_id = arinvt.id
    where
        translog.trans_type in ('PROCESS WIP', 'PACKING SLIP')
        and translog.arinvt_id is not null
    group by
        translog.arinvt_id
        ,arinvt.itemno

)
-- select * from items_last_use_date where itemno = 'PC-300-4NXG-Y_26420' order by arinvt_id desc, trans_date desc;

,items_not_used_since_2021_to_exclude as (
    -- items created before 1/1/2021 and not used since 1/1/2021
    select distinct
        translog.arinvt_id as arinvt_id
        ,arinvt.itemno
        ,arinvt.descrip
        ,nvl(arinvt.pk_hide, 'N') as is_item_inactive
        ,items_creation_date.item_creation_date
        ,nvl(items_last_use_date.recent_trans_date, '1900-01-01') as recent_trans_date
    from
        fivetran_raw.oracle_iqms.translog translog
    left outer join
        fivetran_raw.oracle_iqms.arinvt arinvt on translog.arinvt_id = arinvt.id
    left outer join
        items_creation_date on translog.arinvt_id = items_creation_date.arinvt_id
    left outer join
        items_last_use_date on translog.arinvt_id = items_last_use_date.arinvt_id
    left outer join
        fivetran_raw.oracle_iqms.standard standard on arinvt.standard_id = standard.id

    where
        translog.arinvt_id is not null
        and arinvt.itemno is not null
        and (items_creation_date.item_creation_date < TO_DATE('2021-01-01', 'YYYY-MM-DD')
            and nvl(items_last_use_date.recent_trans_date, '1900-01-01') < TO_DATE('2021-01-01', 'YYYY-MM-DD'))
        or standard.mfgno like '%ECO%'


)
-- select * from items_not_used_since_2021_to_exclude where arinvt_id = '206995' order by arinvt_id desc;
,config_items_to_exclude as (

    select distinct
        ord_detail.arinvt_id
    from
        fivetran_raw.oracle_iqms.ord_detail ord_detail
    left outer join
        fivetran_raw.oracle_iqms.orders orders on ord_detail.orders_id = orders.id
    left outer join
        fivetran_raw.oracle_iqms.crm_quote crm_quote on orders.crm_quote_id = crm_quote.id
    left outer join
        fivetran_raw.oracle_iqms.crm_quote_detail crm_quote_detail on crm_quote.id = crm_quote_detail.crm_quote_id
    where
        crm_quote_detail.id is not null and crm_quote_detail.source = 'SALES_CONFIG'

    union

    select distinct
        hist_ord_detail.arinvt_id
    from
        fivetran_raw.oracle_iqms.hist_ord_detail hist_ord_detail
    left outer join
        fivetran_raw.oracle_iqms.hist_orders hist_orders on hist_ord_detail.orders_id = hist_orders.id
    left outer join
        fivetran_raw.oracle_iqms.crm_quote crm_quote on hist_orders.crm_quote_id = crm_quote.id
    left outer join
        fivetran_raw.oracle_iqms.crm_quote_detail crm_quote_detail on crm_quote.id = crm_quote_detail.crm_quote_id
    where
        crm_quote_detail.id is not null and crm_quote_detail.source = 'SALES_CONFIG'


)
-- select * from config_items_to_exclude where arinvt_id = '206995';
-- Since there are no distinct/clear way to yield weight per item,
-- it has to be calculated from shipments
,package_weight_details as (

    select
        sp.id as package_id
        ,sp.actual_weight as total_package_weight
        ,od.arinvt_id
        ,od.cumm_shipped as item_quantity
        ,sum(sd.cumm_shipped) over (partition by sp.id) as total_package_quantity
    from
        iqms.public.shipment_packages sp
    join fivetran_raw.oracle_iqms.shipments s
        on sp.shipments_id = s.id
    join fivetran_raw.oracle_iqms.shipment_dtl sd
        on s.id = sd.shipments_id
    join fivetran_raw.oracle_iqms.ord_detail od
        on sd.order_dtl_id = od.id

),distributed_weights as (

    select
        arinvt_id
        ,total_package_weight * (item_quantity / nullif(total_package_quantity, 0))
            as distributed_weight
        ,item_quantity
    from
        package_weight_details

),item_weights as (

    select
        arinvt_id
        ,sum(distributed_weight) as total_weight
        ,sum(item_quantity) as total_quantity
    from
        distributed_weights
    group by
        arinvt_id

),weight_per_unit as (

    select
        i.arinvt_id
        ,a.itemno
        ,a.descrip
        ,i.total_weight / nullif(i.total_quantity, 0) as weight_per_unit
    from
        item_weights i
    left outer join fivetran_raw.oracle_iqms.arinvt a
        on i.arinvt_id = a.id
    order by
        weight_per_unit desc

)

-- main query
,results as (

    select distinct
        dim_inventory.inventory_id        as external_id
        ,dim_inventory.item_number        as item_name_number
        ,dim_inventory.item_description   as display_name
        ,dim_inventory.item_description   as description
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
        ,null                             as preferred_location
        ,'Sub-Assembly'                   as cost_category
        ,null                             as round_up_quantity
        ,null                             as planning_item_category
        ,'TRUE'                           as Use_Bins
        ,'Material Requirements Planning' as replenishment_method
        ,'TRUE'                           as match_bill_to_receipt
        ,null                             as phantom
        ,'FALSE'                          as special_work_order_item
        ,'TRUE'                           as offer_support
        ,weight_per_unit.weight_per_unit  as weight
        ,'lb'                             as weight_uom
        ,'12010 Inventory : Raw Materials' as asset_account
        ,'40000 Gross Sales'              as income_account
        ,'50000 Cost of Goods Sold'       as cogs_account
        ,'Taxable'                        as tax_schedule

    from
        analytics.dbt_core.dim_inventory dim_inventory
    left outer join
        items_not_used_since_2021_to_exclude on dim_inventory.inventory_id = items_not_used_since_2021_to_exclude.arinvt_id
    left outer join
        config_items_to_exclude on dim_inventory.inventory_id = config_items_to_exclude.arinvt_id
    left outer join
        fivetran_raw.oracle_iqms.arinvt arinvt on dim_inventory.inventory_id = arinvt.id
    left outer join
        item_class item_class on dim_inventory.inventory_id = item_class.inventory_id
    left outer join
        netsuite_item_class netsuite_item_class on item_class.netsuite_class = netsuite_item_class.product_group
    left outer join
        max_item_po_date max_item_po_date on dim_inventory.inventory_id = max_item_po_date.arinvt_id
    left outer join
        weight_per_unit on dim_inventory.inventory_id = weight_per_unit.arinvt_id
    where
        items_not_used_since_2021_to_exclude.arinvt_id is null
        and config_items_to_exclude.arinvt_id is null
        and dim_inventory.default_bom_number is not null
        and upper(dim_inventory.item_description) not like ('%TEST%')
        and arinvt.class not like ('%FG%')

    order by dim_inventory.inventory_id asc

)
////////for Inventory Tab////////////
-- select * from results order by item_name_number desc;

,location_results as (

        select
            dim_inventory.inventory_id   as external_id
            ,'Wesley International LLC'  as subsidiary
            ,'Georgia HQ'                as Location_1_Location
            ,null                        as Location_1_Reorder_Point
            ,'Build'                     as Location_1_Supply_Type
            ,null                        as Location_1_Preferred_Stock_Level
            ,case
                when dim_inventory.lead_days = 0
                    then null
                else dim_inventory.lead_days
            end                          as Location_1_Purchase_Lead_Time
            ,case
                when arinvt.safety_stock = 0
                    then null
                else arinvt.safety_stock
            end                          as Location_1_Safety_Stock_Level
            ,case
                when dim_inventory.unit_of_measure not in ('EACH')
                    then 'Minimum Order Quantity'
                else 'Lot For Lot'
            end                          as Location_1_Lot_Sizing_Method
            ,dim_inventory.min_order_qty as min_order_qty
            ,null
            ,null
            ,null
            ,null
            ,null
            ,'120'                       as location_1_count_interval
            ,dim_inventory.standard_cost as Location_1_Default_Cost
        from
            results
        left outer join
            analytics.dbt_core.dim_inventory dim_inventory on results.external_id = dim_inventory.inventory_id
        left outer join
            fivetran_raw.oracle_iqms.arinvt arinvt on dim_inventory.inventory_id = arinvt.id
        order by results.item_name_number desc

)

////////for Locations Tab////////////
-- select * from location_results;

,vendors_results as (

    select
        results.external_id
        ,results.item_name_number
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
                then 'Yes'
            else 'No'
        end                                                      as Preferred
        ,row_number() over (
            partition by results.external_id, arinvt_vendors.vendor_id
            order by arinvt_vendors_breaks.QUAN asc
        ) as rn
    from
        fivetran_raw.oracle_iqms.arinvt_vendors_breaks arinvt_vendors_breaks
    left outer join
        fivetran_raw.oracle_iqms.arinvt_vendors arinvt_vendors
            on arinvt_vendors_breaks.arinvt_vendors_id = arinvt_vendors.id
    left outer join
        results on arinvt_vendors.arinvt_id = results.external_id
    left outer join
        vendor_price on arinvt_vendors_breaks.id = vendor_price.vendor_price_break_id
    where
        vendor_price.arinvt_vendors_id is not null
        and arinvt_vendors_breaks.deactive_date is null
        and results.external_id is not null
        qualify rn = 1
)

////////for Vendors Tab////////////
select * from vendors_results order by item_name_number desc, vendor_external_id;