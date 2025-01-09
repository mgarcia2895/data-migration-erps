with bill_of_materials as (

    select * from miguelg.public.BILL_OF_MATERIALS_FULL_MAIN where external_id is not null

)

,manufacturing_routing_tab as (

    select
        bill_of_materials.external_id || '_Default' as external_id
        ,bill_of_materials.external_id || '_Default' as name
        ,'Wesley International LLC' as Subsidiary
        ,bill_of_materials.external_id as bill_of_materials_external_id
        ,'Georgia HQ' as location_external_id
        ,'TRUE' as is_default
        ,'FALSE' as auto_calculate_lag_time
    from
        bill_of_materials
    order by
        bill_of_materials.name desc

)
-- select * from manufacturing_routing_tab;

,mfg_routing_steps_tab as (

    select
        external_id || '_Default' as external_id
        ,ptoper.opseq * 10 as operation_sequence
        ,sndop.opdesc as operation_name
        ,case
            when
                sndop.cntr_type in ('FINAL ASSY', 'WORK_CELL', 'ASSY')
                    then
                        'CL - EV Work Cell'
            when
                sndop.cntr_type in ('SHIPPING')
                    then
                        'CL - Palletization'
            when
                sndop.cntr_type in ('KITTING')
                    then
                        'CL - Parts Picking'
            when
                sndop.cntr_type in ('FINAL ASSEMBLY-PJ', 'PRE PAINT ASSY')
                    then
                        'CL - PJ Work Cell'
            when
                sndop.cntr_type in ('QUALITY', 'INSPECTION TRAILER', 'INSPECTION EV', 'INSPECTION PJ', 'INSPECTION')
                    then
                        'CL - Quality Control'
            when
                sndop.cntr_type in ('TRAILER ASSEMBLY')
                    then
                        'CL - TR Work Cell'
            when
                sndop.cntr_type in ('FOAM FILLING')
                    then
                        'SD - Bench Work Cell'
            when
                sndop.cntr_type in ('PAINT')
                    then
                        'SD - Paint'
            when
                sndop.cntr_type in ('SAW', 'DRILL', 'MACHINING')
                    then
                        'SD - Saw-Drill'
            when
                sndop.cntr_type in ('WELD')
                    then
                        'SD - Weld'
            else
                null
        end as manufacturing_work_center
        ,case
            when
                sndop.cntr_type in ('FINAL ASSY', 'WORK_CELL', 'ASSY')
                    then
                        'CL - EV Work Cell'
            when
                sndop.cntr_type in ('SHIPPING')
                    then
                        'CL - Palletization'
            when
                sndop.cntr_type in ('KITTING')
                    then
                        'CL - Parts Picking'
            when
                sndop.cntr_type in ('FINAL ASSEMBLY-PJ', 'PRE PAINT ASSY')
                    then
                        'CL - PJ Work Cell'
            when
                sndop.cntr_type in ('QUALITY', 'INSPECTION TRAILER', 'INSPECTION EV', 'INSPECTION PJ', 'INSPECTION')
                    then
                        'CL - Quality Control'
            when
                sndop.cntr_type in ('TRAILER ASSEMBLY')
                    then
                        'CL - TR Work Cell'
            when
                sndop.cntr_type in ('FOAM FILLING')
                    then
                        'SD - Bench Work Cell'
            when
                sndop.cntr_type in ('PAINT')
                    then
                        'SD - Paint'
            when
                sndop.cntr_type in ('SAW', 'DRILL', 'MACHINING')
                    then
                        'SD - Saw-Drill'
            when
                sndop.cntr_type in ('WELD')
                    then
                        'SD - Weld'
            else
                null
        end as manufacturing_cost_template_external_id
        ,null as setup_time
        ,sndop.cycletm as run_rate
    from
        bill_of_materials
    left outer join
        fivetran_raw.oracle_iqms.partno partno on bill_of_materials.internal_id = partno.standard_id
    left outer join
        fivetran_raw.oracle_iqms.ptoper ptoper on partno.id = ptoper.partno_id
    left outer join
        fivetran_raw.oracle_iqms.sndop sndop on ptoper.sndop_id = sndop.id

)
select * from mfg_routing_steps_tab where operation_sequence is not null order by external_id, operation_sequence;