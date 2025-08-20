WITH CTE_consultores AS (

    SELECT DISTINCT

        v.codigo AS "id_consultor",
        v.descricao AS "nome_consultor",
        CASE WHEN cat.nome like '%FTR%'
			THEN cat.nome 
			ELSE cat.fantasia 
		END AS "unidade",
        v.ativo AS "status_ativo",
		'' as empresa


    FROM vendedor v

		LEFT JOIN representative_salesman rsm on rsm.code_salesman = v.codigo
		LEFT JOIN representante r on r.codigo = rsm.CODE_REPRESENTATIVE
		LEFT JOIN catalogo cat on cat.cnpj_cpf = r.cnpj_cpf
)


select

id_consultor,
nome_consultor,
unidade,
status_ativo,
empresa

from CTE_consultores