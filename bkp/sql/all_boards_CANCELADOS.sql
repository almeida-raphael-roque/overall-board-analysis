SELECT DISTINCT
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
    coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
    coalesce(iv.id,it.id,itt.id) as "id_placa",
    coalesce(irsc.ID_VEHICLE) as "id_veiculo",
    coalesce(irsct.ID_TRAILER) as "id_carroceria",
    ir.id as "Matricula",
    irs.id as "Conjunto",
    cata.fantasia as "Unidade",
    iss.description as "Status",
    cat.nome as "Cliente",
    irs.data_registration  as "Data",
    cast(irs.date_cancellation as date) as "data_cancelamento",
    irs.user_name_cancellation as "usuario_cancelamento",
    current_date AS "data_filtro",
    'Segtruck' as empresa
    
from silver.insurance_registration ir
        left outer join silver.insurance_reg_set irs on irs.parent = ir.id
        left outer join silver.cliente clie on clie.codigo = ir.CUSTOMER_ID
        left outer join silver.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
        left outer join silver.representante r on r.codigo = irs.id_unity
        left outer join silver.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
        left outer join silver.insurance_status iss on iss.id = irs.id_status
        left outer join silver.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        left outer join silver.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
        left outer join silver.TIPO_VEICULO tv on tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        left outer join silver.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
        left outer join silver.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
        left outer join silver.insurance_trailer itt on itt.id = irsc.ID_TRAILER
        left outer join (
        select distinct 
            irs.id as conjunto,
            wb.user_name as suporte
    
            from silver.insurance_reg_set irs 
                inner join silver.web_user wb on wb.id = irs.id_user_support
            ) as user_name on user_name.conjunto = irs.id
            

where date_cancellation is not null
and (
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) is not null
    and coalesce(iv.chassi,it.chassi,itt.chassi) is not null
)


-------------------------------------------------------------------------------------------

union all

-------------------------------------------------------------------------------------------


SELECT DISTINCT
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
    coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
    coalesce(iv.id,it.id,itt.id) as "id_placa",
    coalesce(irsc.ID_VEHICLE) as "id_veiculo",
    coalesce(irsct.ID_TRAILER) as "id_carroceria",
    ir.id as "Matricula",
    irs.id as "Conjunto",
    cata.fantasia as "Unidade",
    iss.description as "Status",
    cat.nome as "Cliente",
    irs.data_registration  as "Data",
    cast(irs.date_cancellation as date) as "data_cancelamento",
    irs.user_name_cancellation as "usuario_cancelamento",
    current_date AS "data_filtro",
    'Stcoop' as empresa
    
from stcoop.insurance_registration ir
        left outer join stcoop.insurance_reg_set irs on irs.parent = ir.id
        left outer join stcoop.cliente clie on clie.codigo = ir.CUSTOMER_ID
        left outer join stcoop.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
        left outer join stcoop.representante r on r.codigo = irs.id_unity
        left outer join stcoop.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
        left outer join stcoop.insurance_status iss on iss.id = irs.id_status
        left outer join stcoop.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        left outer join stcoop.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
        left outer join stcoop.TIPO_VEICULO tv on tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        left outer join stcoop.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
        left outer join stcoop.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
        left outer join stcoop.insurance_trailer itt on itt.id = irsc.ID_TRAILER
        left outer join (
        select distinct 
            irs.id as conjunto,
            wb.user_name as suporte
    
            from stcoop.insurance_reg_set irs 
                inner join stcoop.web_user wb on wb.id = irs.id_user_support
            ) as user_name on user_name.conjunto = irs.id
            

where date_cancellation is not null
and (
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) is not null
    and coalesce(iv.chassi,it.chassi,itt.chassi) is not null
)

-------------------------------------------------------------------------------------------

union all

-------------------------------------------------------------------------------------------


SELECT DISTINCT
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
    coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
    coalesce(iv.id,it.id,itt.id) as "id_placa",
    coalesce(irsc.ID_VEHICLE) as "id_veiculo",
    coalesce(irsct.ID_TRAILER) as "id_carroceria",
    ir.id as "Matricula",
    irs.id as "Conjunto",
    cata.fantasia as "Unidade",
    iss.description as "Status",
    cat.nome as "Cliente",
    irs.data_registration  as "Data",
    cast(irs.date_cancellation as date) as "data_cancelamento",
    irs.user_name_cancellation as "usuario_cancelamento",
    current_date AS "data_filtro",
    'Viavante' as empresa
    
from viavante.insurance_registration ir
        left outer join viavante.insurance_reg_set irs on irs.parent = ir.id
        left outer join viavante.cliente clie on clie.codigo = ir.CUSTOMER_ID
        left outer join viavante.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
        left outer join viavante.representante r on r.codigo = irs.id_unity
        left outer join viavante.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
        left outer join viavante.insurance_status iss on iss.id = irs.id_status
        left outer join viavante.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        left outer join viavante.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
        left outer join viavante.TIPO_VEICULO tv on tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        left outer join viavante.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
        left outer join viavante.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
        left outer join viavante.insurance_trailer itt on itt.id = irsc.ID_TRAILER
        left outer join (
        select distinct 
            irs.id as conjunto,
            wb.user_name as suporte
    
            from viavante.insurance_reg_set irs 
                inner join viavante.web_user wb on wb.id = irs.id_user_support
            ) as user_name on user_name.conjunto = irs.id
            

where date_cancellation is not null
and (
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) is not null
    and coalesce(iv.chassi,it.chassi,itt.chassi) is not null
)
