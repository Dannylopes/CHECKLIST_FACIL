# Diagrama de tabelas - Camada GOLD

A modelagem abaixo representa os relacionamentos lógicos das tabelas Gold do projeto CHECKLIST_FACIL.

```mermaid
erDiagram
    CHECKLISTFACIL_DIM_STATUS {
        int sk_status PK
        int id_original_status
        string nome_status
    }

    CHECKLISTFACIL_DIM_PLATAFORMA {
        int sk_plataforma PK
        int id_original_plataforma
        string nome_plataforma
    }

    CHECKLISTFACIL_DIM_CHECKLISTS {
        int sk_checklist PK
        int id_original_checklist
        string nome_checklist
        string status_checklist
    }

    CHECKLISTFACIL_DIM_UNIDADES {
        int sk_unidade PK
        int id_original_unidade
        string nome_unidade
        string status_unidade
    }

    CHECKLISTFACIL_DIM_USUARIO {
        int sk_usuario PK
        int id_original_usuario
        string nome_usuario
        string status_usuario
        string tipo_usuario
    }

    CHECKLISTFACIL_DIM_CATEGORIAS {
        int sk_categoria PK
        int id_original_categoria
        int id_checklist
        int id_categoria_pai
        string nome_categoria
    }

    CHECKLISTFACIL_DIM_ITENS {
        int sk_item PK
        int id_original_item
        int id_checklist
        int id_categoria
        string nome_item
    }

    CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS {
        int sk_checklist_aplicado PK
        int id_original_checklist_aplicado
        int sk_status FK
        int sk_plataforma FK
        int sk_checklist FK
        int sk_unidade FK
        int sk_usuario FK
        decimal pontuacao
    }

    CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS_RESULTADOS {
        int sk_resultado PK
        int id_original_resultado
        int sk_checklist_aplicado FK
        int sk_categoria FK
        int sk_item FK
        decimal peso_obtido
    }

    CHECKLISTFACIL_DIM_STATUS ||--o{ CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS : sk_status
    CHECKLISTFACIL_DIM_PLATAFORMA ||--o{ CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS : sk_plataforma
    CHECKLISTFACIL_DIM_CHECKLISTS ||--o{ CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS : sk_checklist
    CHECKLISTFACIL_DIM_UNIDADES ||--o{ CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS : sk_unidade
    CHECKLISTFACIL_DIM_USUARIO ||--o{ CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS : sk_usuario

    CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS ||--o{ CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS_RESULTADOS : sk_checklist_aplicado
    CHECKLISTFACIL_DIM_CATEGORIAS ||--o{ CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS_RESULTADOS : sk_categoria
    CHECKLISTFACIL_DIM_ITENS ||--o{ CHECKLISTFACIL_FATO_CHECKLISTS_APLICADOS_RESULTADOS : sk_item
```

## Notas

- O relacionamento entre `dim_itens` e `dim_categorias` também existe de forma lógica por `id_categoria`, embora não esteja materializado por `sk` na fato principal.
- O relacionamento entre `dim_categorias` e `dim_checklists` também é lógico por `id_checklist`.
- Todas as `sk_*` são derivadas por `crc32` do ID original nos notebooks Gold.
