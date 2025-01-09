with assembly_items_comb as (

    select * from miguelg.public.serialized_assembly_items

    union

    select * from miguelg.public.non_serialized_assembly_items

)


,all_associated_boms as (

    select distinct
        assembly_items_comb.external_id as assembly_id
        ,assembly_items_comb."Item Name/Number" as assembly_item_num
        ,assembly_items_comb.description as assembly_item_descrip
        ,standard.id as bom_id
        ,case
            when arinvt.standard_id = standard.id
                then 'Default'
            else null
        end as is_bom_default
        ,standard.mfgno as bom_mfg_num
        ,standard.descrip as bom_descrip
        ,nvl(max(case
                when translog.trans_type in ('PROCESS WIP', 'PACKING SLIP')
                then translog.trans_date
            end), '1900-01-01') as last_transaction_date
    from
        fivetran_raw.oracle_iqms.partno partno
    left outer join
        assembly_items_comb on partno.arinvt_id = assembly_items_comb.external_id
    left outer join
        fivetran_raw.oracle_iqms.arinvt arinvt on assembly_items_comb.external_id = arinvt.id
    left outer join
        fivetran_raw.oracle_iqms.standard standard on partno.standard_id = standard.id
    left outer join
        fivetran_raw.oracle_iqms.translog translog on standard.id = translog.standard_id
    where
        UPPER(standard.mfgno) not like '%SUB%' and
        UPPER(standard.mfgno) not like '%TEST%' and
        UPPER(standard.mfgno) not like '%ECO%' and
        UPPER(standard.mfgno) not like '%REWORK%' and
        assembly_items_comb.external_id is not null
    group by
        assembly_items_comb.external_id
        ,assembly_items_comb."Item Name/Number"
        ,assembly_items_comb.description
        ,standard.id
        ,arinvt.standard_id
        ,standard.mfgno
        ,standard.descrip
    order by
        assembly_items_comb."Item Name/Number" desc

)

,filtered_boms as (

    select
        assembly_id
        ,count(bom_id) as total_bom_count
        ,sum(case when is_bom_default = 'Default' then 1 else 0 end) as default_bom_count
    from
        all_associated_boms
    group by
        assembly_id

)

,filtered_associated_boms as (

    select
        all_associated_boms.*
    from
        all_associated_boms
    join
        filtered_boms on all_associated_boms.assembly_id = filtered_boms.assembly_id
    where
        -- 1. Keep assembly IDs with a single BOM ID
        filtered_boms.total_bom_count = 1

        -- 2. For assembly IDs with multiple BOM IDs, yield only 'Default' BOMs
        or (filtered_boms.total_bom_count > 1 and all_associated_boms.is_bom_default = 'Default')

        -- 3. Exclude assembly IDs with multiple BOM IDs but no 'Default'
        and not (filtered_boms.total_bom_count > 1 and filtered_boms.default_bom_count = 0)
    order by
        all_associated_boms.assembly_item_num desc

)
-- select * from filtered_associated_boms;
-- Bill of Materials Tab
,bill_of_materials_tab as (

    select
        assembly_item_num || '-' || 'BOM' as external_id
        ,bom_id as internal_id
        ,assembly_item_num || '-' || 'BOM' as name
        ,'FALSE' as use_component_yield
        ,'FALSE' as available_for_all_assemblies
        ,assembly_id as restrict_to_assemblies_external_id
        ,'TRUE' as available_for_all_locations
        ,null as restrict_to_locations
        ,'Wesley International LLC' as subsidiary
        ,'FALSE' as include_children
    from
        filtered_associated_boms

)
-- select * from bill_of_materials_tab order by external_id;

,bills_of_material_revision_tab as (

    select distinct
        assembly_item_num || '_Rev_00' as external_id
        ,assembly_item_num || '_Rev_00' as name
        ,assembly_item_num || '-' || 'BOM' as bill_of_materials_external_id
        ,'05/04/1900' as effective_start_date
    from
        filtered_associated_boms

)
-- select * from bills_of_material_revision_tab;

,bom_revision_components_tab as (

    select
        filtered_associated_boms.assembly_item_num || '_Rev_00' as external_id
        ,arinvt.id as item_external_id
        ,'100' as component_yield
        ,opmat.ptsper as BOM_quantity
        ,'STOCK' as item_source
    from
        filtered_associated_boms
    left outer join
        fivetran_raw.oracle_iqms.standard standard on filtered_associated_boms.bom_id = standard.id
    left outer join
        fivetran_raw.oracle_iqms.partno partno on standard.id = partno.standard_id
    left outer join
        fivetran_raw.oracle_iqms.ptoper ptoper on partno.id = ptoper.partno_id
    left outer join
        fivetran_raw.oracle_iqms.sndop sndop on ptoper.sndop_id = sndop.id
    left outer join
        fivetran_raw.oracle_iqms.opmat opmat on sndop.id = opmat.sndop_id
    left outer join
        fivetran_raw.oracle_iqms.arinvt arinvt on opmat.arinvt_id = arinvt.id
    where
        arinvt.id is not null --and items_not_used_since_2021.arinvt_id is not null
    order by
        filtered_associated_boms.assembly_item_num || '_Rev_00' desc, arinvt.id asc

)
-- select * from bom_revision_components_tab;

,item_tab as (

    select
        assembly_id as assembly_item_external_id
        ,assembly_item_num || '-' || 'BOM' as bill_of_material_external_id
        ,'TRUE' as master_default
    from
        filtered_associated_boms
)
select * from item_tab order by bill_of_material_external_id desc;