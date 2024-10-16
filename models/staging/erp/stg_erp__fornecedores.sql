with
    fonte_fornecedores as (
        select
            cast(id as int) as pk_fornecedor
            , cast(companyname as varchar) as nome_fornecedor
            , cast(city as varchar) as cidade_fornecedor
            , cast(country as varchar) as pais_fornecedor
        from {{ source('erp_northwind', 'supplier') }}
    )

select *
from fonte_fornecedores
