WITH CTE_consultores_SEG AS (

    SELECT DISTINCT

        v.codigo AS "id_consultor",
        v.descricao AS "nome_consultor",
        cat.fantasia AS "unidade",
		'Segtruck' as empresa


    FROM vendedor v

		LEFT JOIN representative_salesman rsm on rsm.code_salesman = v.codigo
		LEFT JOIN representante r on r.codigo = rsm.CODE_REPRESENTATIVE
		LEFT JOIN catalogo cat on cat.cnpj_cpf = r.cnpj_cpf
),

CTE_consultores_STCOOP AS (

    SELECT DISTINCT

        v.codigo AS "id_consultor",
        v.descricao AS "nome_consultor",
        cat.fantasia AS "unidade",
		'Stcoop' as empresa


    FROM vendedor v

		LEFT JOIN representative_salesman rsm on rsm.code_salesman = v.codigo
		LEFT JOIN representante r on r.codigo = rsm.CODE_REPRESENTATIVE
		LEFT JOIN catalogo cat on cat.cnpj_cpf = r.cnpj_cpf
),


CTE_consultores_VIAVANTE AS (

    SELECT DISTINCT

        v.codigo AS "id_consultor",
        v.descricao AS "nome_consultor",
        cat.fantasia AS "unidade",
		'Viavante' as empresa


    FROM vendedor v

		LEFT JOIN representative_salesman rsm on rsm.code_salesman = v.codigo
		LEFT JOIN representante r on r.codigo = rsm.CODE_REPRESENTATIVE
		LEFT JOIN catalogo cat on cat.cnpj_cpf = r.cnpj_cpf
),


CTE_consultores_TAG AS (

    SELECT DISTINCT

        v.codigo AS "id_consultor",
        v.descricao AS "nome_consultor",
        cat.fantasia AS "unidade",
		'Tag' as empresa


    FROM vendedor v

		LEFT JOIN representative_salesman rsm on rsm.code_salesman = v.codigo
		LEFT JOIN representante r on r.codigo = rsm.CODE_REPRESENTATIVE
		LEFT JOIN catalogo cat on cat.cnpj_cpf = r.cnpj_cpf
)




select

id_consultor,
nome_consultor,
unidade,
empresa

from CTE_consultores_SEG

-------------------------------------------------------------------------

UNION

-------------------------------------------------------------------------

select

id_consultor,
nome_consultor,
unidade,
empresa

from CTE_consultores_STCOOP

-------------------------------------------------------------------------

UNION

-------------------------------------------------------------------------

select

id_consultor,
nome_consultor,
unidade,
empresa

from CTE_consultores_VIAVANTE


-------------------------------------------------------------------------

UNION

-------------------------------------------------------------------------

select

id_consultor,
nome_consultor,
unidade,
empresa

from CTE_consultores_TAG