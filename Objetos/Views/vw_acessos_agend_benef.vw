CREATE OR REPLACE FORCE VIEW VW_ACESSOS_AGEND_BENEF AS
SELECT P.NOM_PESSOA_FISICA AS NOME_BENEFICIARIO,
           P.NUM_CPF           AS CPF_BENEFICIARIO,
           P.DAT_NASC          AS DATA_NASC_BENEFICIARIO,
           FNC_FORMATA_TELEFONE(P.NUM_TEL_FONE1) AS TELEFONE,
           TIPOLOG.NOM_TIPO    AS TIPO_LOGRADOURO,
           END.NOM_LOGRADOURO  AS LOGRADOURO,
           END.NUM_NUMERO      AS NUMERO,
           BAIRRO.NOM_BAIRRO   AS BAIRRO,
           MUNICIPIO.NOM_ABREVIADO AS MUNICIPIO,
           END.NUM_CEP         AS CEP,
           ESTADO.NAME         AS ESTADO,
           P.EMAIL             AS EMAIL,
           COUNT(DISTINCT A.ID) + COUNT(DISTINCT AH.ID ) AS QTD_AGENDAMENTO,
           COUNT(DISTINCT LOG.ID ) AS QTD_ACESSO
    FROM  RECENSEAMENTO.TB_PESSOA_FISICA P
        INNER JOIN RECENSEAMENTO.TB_BENEFICIARIO B
            ON ( P.COD_IDE_CLI = B.COD_IDE_CLI )
        LEFT JOIN RECENSEAMENTO.TB_END_PESSOA_FISICA END
            ON ( P.COD_IDE_CLI = END.COD_IDE_CLI )
           AND ( END.COD_TIPO_END = 601)
        INNER JOIN RECENSEAMENTO.TB_USUARIO USU
            ON ( P.ID = USU.PESSOA_FISICA_ID )
        LEFT JOIN RECENSEAMENTO.TB_AGENDAMENTO A
            ON ( P.ID = A.PESSOA_FISICA_ID )
        LEFT JOIN RECENSEAMENTO.TB_END_TIPO_LOGRADOURO TIPOLOG
            ON ( TIPOLOG.ID = END.COD_TIP_LOGRADOURO )
        LEFT JOIN RECENSEAMENTO.TB_END_BAIRRO BAIRRO
            ON ( END.COD_BAIRRO = BAIRRO.ID )
        LEFT JOIN RECENSEAMENTO.TB_END_LOCALIDADE MUNICIPIO
            ON ( END.COD_MUNICIPIO = MUNICIPIO.ID )
        LEFT JOIN RECENSEAMENTO.TB_DOMINIO_DETALHE ESTADO
            ON (    ESTADO.DOMAIN_ID = 3
                AND ESTADO.ID = END.COD_UF )
        LEFT JOIN RECENSEAMENTO.TB_LOG LOG
            ON ( USU.ID = LOG.USUARIO_ID )
        LEFT JOIN RECENSEAMENTO.TB_AGENDAMENTO_HISTORICO AH
            ON ( P.ID = AH.PESSOA_FISICA_ID )
   WHERE nvl(P.FLG_EXCLUIDO,'N') <> 'S'
    group by P.NOM_PESSOA_FISICA,
             P.NUM_CPF,
             P.DAT_NASC,
             FNC_FORMATA_TELEFONE(P.NUM_TEL_FONE1),
             TIPOLOG.NOM_TIPO,
             END.NOM_LOGRADOURO,
             END.NUM_NUMERO,
             BAIRRO.NOM_BAIRRO,
             MUNICIPIO.NOM_ABREVIADO,
             END.NUM_CEP,
             ESTADO.NAME,
             P.EMAIL
    ORDER BY P.NOM_PESSOA_FISICA;

