CREATE OR REPLACE FORCE VIEW VW_RESUMO_CENSO_ATIVOS_2_FASE AS
WITH Q1 as ((select count(*) as total
               from TB_USUARIO A1
              WHERE EXISTS (SELECT 1 FROM TB_USUARIO_PERFIL UP WHERE UP.USUARIO_ID = A1.ID AND UP.PERFIL_ID = 2)
                AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A1.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')
                AND nvl(trunc(A1.dat_final_pre_cad),to_date('17/12/2018','dd/mm/yyyy')) >= to_date('17/12/2018','dd/mm/yyyy')))
SELECT drive.etapa ,
       drive.PROCESSO ,
       drive.TOTAL,
       drive.percentual_conclusao,
       drive.percentual_remanescente
  FROM (
         SELECT 1 ETAPA,
         'FINALIZOU PRESENCIAL' PROCESSO,
          COUNT(1) TOTAL
          ,round(round(100 * COUNT(1),2)/max(Q1.total),2) percentual_conclusao
          ,100-(round(round(100 * COUNT(1),2)/max(Q1.total),2)) percentual_remanescente
           FROM TB_PROTOCOLO PRO, Q1
           WHERE  EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.COD_IDE_CLI = PRO.COD_IDE_CLI AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')
          GROUP BY 1,'FINALIZOU PRESENCIAL'
        UNION
          SELECT 2 ETAPA,
          'ALTEROU DADOS PESSOAIS' PROCESSO,
          COUNT(1) TOTAL
          ,round(round(100 * COUNT(1),2)/max(Q1.total),2)    percentual_conclusao
          ,100-(round(round(100 * COUNT(1),2)/max(Q1.total),2))    percentual_remanescente
            FROM TB_PESSOA_FISICA, Q1
           WHERE FLAG_RECENSEAMENTO = 1
             AND NVL(FLG_EXCLUIDO,'N') <> 'S'
           group by 2, 'ALTEROU DADOS PESSOAIS'
        UNION
            SELECT 3 ETAPA,
           'INCLUIU / ALTEROU ENDERECO' PROCESSO,
            COUNT(1) TOTAL
            ,round(round(100 * COUNT(1),2)/max(Q1.total),2) percentual_conclusao
            ,100-(round(round(100 * COUNT(1),2)/max(Q1.total),2)) percentual_remanescente
            FROM TB_USUARIO A, Q1
           WHERE EXISTS (SELECT 1 FROM TB_END_PESSOA_FISICA EP, TB_PESSOA_FISICA PF WHERE EP.COD_IDE_CLI = PF.COD_IDE_CLI AND EP.FLAG_RECENSEAMENTO = 1 AND PF.ID = A.PESSOA_FISICA_ID)
             AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')
           group by 3, 'INCLUIU / ALTEROU ENDERECO'
        UNION
        SELECT 4 ETAPA
             , 'INCLUIU DEPENDENTE' PROCESSO
             , COUNT(1) TOTAL
             , round(round(100 * COUNT(1),2)/max(Q1.total),2)  percentual_conclusao
             , 100-(round(round(100 * COUNT(1),2)/max(Q1.total),2)) percentual_remanescente
          FROM TB_USUARIO A, Q1
         WHERE EXISTS (SELECT 1 FROM TB_USUARIO_PERFIL UP WHERE UP.USUARIO_ID = A.ID AND UP.PERFIL_ID = 2)
           AND EXISTS (SELECT 1 FROM TB_DEPENDENTE     DP, TB_PESSOA_FISICA PF WHERE DP.COD_IDE_CLI = PF.COD_IDE_CLI AND DP.FLAG_RECENSEAMENTO = 1 AND PF.ID = A.PESSOA_FISICA_ID )
           AND EXISTS (SELECT 1 FROM TB_BENEFICIARIO   BE, TB_PESSOA_FISICA PF WHERE BE.COD_IDE_CLI = PF.COD_IDE_CLI AND PF.ID = A.PESSOA_FISICA_ID AND BE.COD_TIPO_BENEFICIO <> '1')
           AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')
            group by 4, 'INCLUIU DEPENDENTE'
        UNION
        SELECT 5 ETAPA
             , 'INCLUIU REPRESENTANTE LEGAL' PROCESSO
             , COUNT(1) TOTAL
             , round(round(100 * COUNT(1),2)/max(Q1.total),2)  percentual_conclusao
             , 100 - round(round(100 * COUNT(1),2)/max(Q1.total),2) percentual_remanescente
          FROM TB_USUARIO A, Q1
         WHERE EXISTS (SELECT 1 FROM TB_USUARIO_PERFIL UP WHERE UP.USUARIO_ID = A.ID AND UP.PERFIL_ID = 2)
           AND EXISTS (SELECT 1 FROM TB_REPRESENTANTE_LEGAL BE, TB_PESSOA_FISICA PF WHERE BE.COD_IDE_CLI = PF.COD_IDE_CLI AND BE.FLAG_RECENSEAMENTO = 1 AND PF.ID = A.PESSOA_FISICA_ID)
           AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')
            group by 5, 'INCLUIU REPRESENTANTE LEGAL', 0
) DRIVE
ORDER BY DRIVE.ETAPA;

