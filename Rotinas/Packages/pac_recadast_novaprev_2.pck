CREATE OR REPLACE PACKAGE PAC_RECADAST_NOVAPREV_2 is

    /* -----------------------------------------------------------------------------------------------
    Descricao: Package de rotinas utilizadas no recadastramento, fazendo a carga de dados do
    schema USER_IPESP para o schema RECENSEAMENTO

    Vers?o 1.08

    Historico de vers?es
    - 1.08 - 29/03/2019 - Francisco Cavalcante - Alterac?o em SP_CARGA_MASSIVA_RECADAST para selecionar registros por
                                                 mes de nascimento conforme intervalo de meses recebido por parametro.
                                               - Criac?o de SP_REGISTRA_ACESSO_USUARIO para fazer a inclus?o da pessoa 
                                                 fisica em TB_USUARIO e TB_USUARIO_PERFIL.
                                               - Alterac?o em SP_CARGA_DADOS_RECADAST para cadastrar o acesso ao sistema
                                                 via SP_REGISTRA_ACESSO_USUARIO.
    - 1.07 - 01/03/2019 - Francisco Cavalcante - Card US53912: Alterar SP_CARGA_MASSIVA_RECADAST para considerar
                                                 o segundo e o terceiro mes apos o mes atual como meses de referencia
                                                 para a carga.
    - 1.06 - 27/02/2019 - Francisco Cavalcante - Ajuste em SP_CARGA_DADOS_RECADAST para obter no maximo um registro 
                                                 de TB_INFORMACAO_BANCARIA, cursor PF.
    - 1.05 - 20/12/2018 - Francisco Cavalcante - Alterac?o em SP_CARGA_DADOS_RECADAST para preencher a coluna
                                                 RECENSEAMENTO.TB_PESSOA_FISICA.FLAG_UNIAO_ESTAVEL com o valor 'N' se
                                                 a coluna USER_IPESP.TB_PESSOA_FISICA.FLG_UNIAO_ESTAVEL estiver nula.
    - 1.04 - 26/11/2018 - Francisco Cavalcante - Alterac?o em SP_CARGA_DADOS_RECADAST para tratamento da coluna
                                                 TB_BENEFICIARIO.DAT_CONCESSAO.
    - 1.03 - 31/10/2018 - Francisco Cavalcante - Alterac?o em SP_CARGA_DADOS_RECADAST para definir os valores padr?o
                                                 de FLAG_RECADASTRAMENTO, FLAG_RECENSEAMENTO, DAT_RECADASTRAMENTO e
                                                 DAT_RECENSEAMENTO ao inserir registro em RECENSEAMENTO.TB_BENEFICIARIO.
    - 1.02 - 22/10/2018 - Francisco Cavalcante - Alterac?o em SP_CARGA_DADOS_RECADAST para definir o valor de 
                                                 RECENSEAMENTO.TB_BENEFICIARIO.COD_TIPO_BENEFICIO = '4' quando  
                                                 USER_IPESP.TB_CONCESSAO_BENEFICIO.COD_TIPO_BENEFICIO = 'G' 
                                                 para o registro sendo tratado.
    - 1.01 - 02/10/2018 - Francisco Cavalcante - Alterac?o em SP_CARGA_DADOS_RECADAST para incluir tratamento para as 
                                                 colunas FLG_COMPLEMENTO_INSS e FLG_PECULIO de 
                                                 RECENSEAMENTO.TB_BENEFICIARIO.
    - 1.00 - 04/09/2019 - Francisco Cavalcante - Criac?o do objeto.
    ----------------------------------------------------------------------------------------------- */

   type typ_rec_ben_pendente is record
   (NUM_PRONTUARIO    tb_beneficiario.num_prontuario%type,
    NOM_BENEFICIARIO  tb_pessoa_fisica.nom_pessoa_fisica%type,
    NUM_CPF           tb_pessoa_fisica.num_cpf%type,  
    TIPO_BENEFICIO    tb_dominio_detalhe.name%type,  
    DAT_CONCESSAO     tb_beneficiario.dat_concessao%type,    
    TIP_LOGRADOURO    tb_end_tipo_logradouro.nom_tipo%type,
    LOGRADOURO        tb_end_logradouro.nom_logradouro%type,    
    NUMERO            tb_end_pessoa_fisica.num_numero%type,    
    BAIRRO            tb_end_bairro.nom_bairro%type,
    MUNICIPIO         tb_end_localidade.nom_localidade%type,
    CEP               tb_end_pessoa_fisica.num_cep%type,
    ESTADO            tb_dominio_detalhe.Cod_Domain_Detail%type,
    TELEFONE          tb_pessoa_fisica.num_tel_fone1%type,
    DAT_NASC          tb_pessoa_fisica.dat_nasc%type,
    FLG_SIT_RECAD     char,
    DES_SIT_RECAD     tb_dominio_detalhe.name%type);
    
    type typ_tab_ben_pendente is table of typ_rec_ben_pendente;

    v_role_user_id          recenseamento.tb_perfil.id%type; 

    PROCEDURE SP_CARGA_MASSIVA_RECADAST (i_mes_ini number, 
                                         i_mes_fim number);


    PROCEDURE SP_CARGA_DADOS_RECADAST (i_cod_ins     in     number,
                                       i_cod_ide_cli in     varchar2,
                                       o_msg         in out varchar2);

    PROCEDURE SP_REGISTRA_ACESSO_USUARIO (i_pessoa_fisica_id in recenseamento.tb_usuario.pessoa_fisica_id%type,
                                          i_username         in recenseamento.tb_usuario.username%type,
                                          o_msg              in out varchar2);

    FUNCTION FNC_PERM_CARGA_PESSOA_RECAD (i_cod_ins     in number,
                                          i_cod_ide_cli in varchar) RETURN VARCHAR2;
                                          
    -- Rotina que Atualiza a data de obito do recenseamento a partir da novaprev.    
    PROCEDURE sp_atu_pf_dat_obito(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                  i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                  o_cod_erro   out number,
                                  o_msg_erro   out varchar2);    
                                  
    -- Rotina que retorna dados do relatorio de Beneficiarios pendentes de Recadastramento.
    FUNCTION fnc_ret_rel_ben_pendente (i_nom_pessoa_fisica in varchar2,
                                       i_num_cpf           in varchar2,
                                       i_num_matricula     in varchar2, 
                                       i_mes_aniversario   in varchar2) return typ_tab_ben_pendente pipelined;

    -- Rotina que remove  todos os agendamentos de uma pessoa fisica    
    PROCEDURE sp_remove_agendamento_pf(i_id_pf in tb_pessoa_fisica.id%type,                                                                                                            
                                       o_cod_erro out number,
                                       o_msg_erro out varchar2);
                                       
    PROCEDURE sp_atu_pf_ben_cessado(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                    i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                    o_cod_erro   out number,
                                    o_msg_erro   out varchar2);     
                                    
    PROCEDURE sp_atu_pf_ben_2019(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                 i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                 o_cod_erro   out number,
                                 o_msg_erro   out varchar2);                                  

END PAC_RECADAST_NOVAPREV_2;
/

CREATE OR REPLACE PACKAGE BODY PAC_RECADAST_NOVAPREV_2 is

    
    -----------------------------------------------------------------------------------------------

    PROCEDURE SP_CARGA_MASSIVA_RECADAST (i_mes_ini number, 
                                         i_mes_fim number) IS

        v_msg_carga     varchar2(1024);
        ERRO_PARAMETROS EXCEPTION;
        ERRO_CARGA      EXCEPTION;
        qtd_processado  number := 0;
        v_lista_mes     varchar2(50) := '#';
        v_cur_mes       number := 0;

    BEGIN

        if (   (nvl(i_mes_ini, 0) not between 1 and 12)
            or (nvl(i_mes_fim, 0) not between 1 and 12)   ) then
            raise ERRO_PARAMETROS;
        end if;

        v_cur_mes := i_mes_ini;

        loop
            v_lista_mes := v_lista_mes ||trim(to_char(v_cur_mes, '00'))||'#';
            exit when v_cur_mes = i_mes_fim;
            v_cur_mes := v_cur_mes + 1;
            if (v_cur_mes > 12) then
                v_cur_mes := 1;
            end if;
        end loop;

        recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_MASSIVA_RECADAST',
                                            'I_MES_INI: '||i_mes_ini||', I_MES_FIM: '||i_mes_fim,
                                            'Iniciando carga de dados para o(s) mes(es) '||v_lista_mes);

        for pf in (select distinct a.cod_ins, a.cod_ide_cli
                   from user_ipesp.tb_pessoa_fisica a
                       inner join user_ipesp.tb_beneficiario b
                           on     a.cod_ins     = b.cod_ins
                              and a.cod_ide_cli = b.cod_ide_cli_ben
                              and upper(b.flg_status) in ('A', 'S')
                              and nvl(b.dat_fim_ben, (sysdate + 1)) > sysdate
                   where a.dat_nasc is not null
                     and instr(v_lista_mes, to_char(a.dat_nasc, 'MM')) > 0
                     and not exists (select 'x'
                                     from recenseamento.tb_pessoa_fisica c
                                     where c.cod_ins     = a.cod_ins
                                       and c.cod_ide_cli = a.cod_ide_cli
                                       and (   (   nvl(c.flag_recadastramento, 0) > 0
                                                or c.dat_recadastramento is not null) ) )
                     and not exists (select 'x'
                                     from recenseamento.tb_beneficiario d
                                     where d.cod_ins     = a.cod_ins
                                       and d.cod_ide_cli = a.cod_ide_cli
                                       and (   (   nvl(d.flag_recadastramento, 0) > 0
                                                or d.dat_recadastramento is not null) ) )
                   ) loop

            sp_carga_dados_recadast(pf.cod_ins, pf.cod_ide_cli, v_msg_carga);

            --if (v_msg_carga is not null) then
            --    raise ERRO_CARGA;
            --end if;

            qtd_processado := qtd_processado + 1;

            if (qtd_processado > 100) then
                commit;
                qtd_processado := 0;
            end if;

        end loop;

        commit;

    EXCEPTION
        WHEN ERRO_PARAMETROS THEN
            raise_application_error(-20001, 'Os parametros de entrada n?o est?o preenchidos corretamente:'||
                                                ' mes inicial: '||i_mes_ini||', mes final: '||i_mes_fim);

        WHEN ERRO_CARGA THEN
            rollback;
            raise_application_error(-20002, v_msg_carga);

        WHEN OTHERS THEN    
            rollback;
            raise_application_error(-20003, sqlerrm);

    END SP_CARGA_MASSIVA_RECADAST;

    -----------------------------------------------------------------------------------------------

    PROCEDURE SP_CARGA_DADOS_RECADAST (i_cod_ins     in number,
                                       i_cod_ide_cli in varchar2,
                                       o_msg         in out varchar2)
    IS

        v_passo                 number := 0;
        v_qtd_cid_nasc          number;
        v_cod_cid_nasc          recenseamento.tb_pessoa_fisica.cod_cid_nasc%type;
        v_cod_pais_nasc         recenseamento.tb_pessoa_fisica.cod_pais_nasc%type;
        v_id_recen              recenseamento.tb_pessoa_fisica.id%type;
        v_usu_existe            varchar2(5);
        v_msg                   varchar2(4000);

    BEGIN

        savepoint sp1;

        v_passo := 0;

        if (v_role_user_id is null) then
            select id
            into v_role_user_id
            from recenseamento.tb_perfil
            where authority = 'ROLE_USER'; 
        end if;

        if ( fnc_perm_carga_pessoa_recad(i_cod_ins, i_cod_ide_cli) = 'TRUE' ) then

            v_passo := 1;

            for pf in (select a.num_cpf, a.cod_est_civ, a.cod_ide_cli, a.cod_ins, a.cod_nacio, a.cod_org_emi_rg, 
                              a.cod_raca, a.cod_reg_casamento, a.cod_sexo, a.cod_uf_emi_rg, a.cod_uf_nasc, 
                              a.dat_emi_rg, a.dat_nasc, a.dat_obito, a.dat_recenseamento, a.des_email, 
                              a.flg_uniao_estavel, a.nom_mae, a.nom_pai, a.nom_pessoa_fisica, a.login,
                              a.num_cartorio_nasc, a.num_cer_res, a.num_folha_nasc, a.num_livro_nasc, 
                              a.num_rg, a.num_sec_ele, a.num_tel_1, a.num_tel_2, a.num_tit_ele, a.num_zon_ele,
                              a.tip_num_tel_1, a.tip_num_tel_2, a.dat_cheg_pais, a.des_natural, a.dat_ult_atu,
                              b.num_nit_inss, b.num_pis,
                              c.num_dv_agencia, c.num_dv_conta, c.num_agencia, c.cod_banco, c.num_conta,
                              e.dat_nasc dat_nasc_conjuge, e.nom_pessoa_fisica nom_conjuge, e.num_cpf num_cpf_conjuge,
                              f.cod_ide_cli cod_ide_cli_recen, f.flag_recadastramento, f.dat_recadastramento, 
                              f.dat_ult_atu dat_ult_atu_recen, f.id id_recen
                       from user_ipesp.tb_pessoa_fisica a
                          left outer join user_ipesp.tb_servidor b
                              on      a.cod_ins     = b.cod_ins
                                  and a.cod_ide_cli = b.cod_ide_cli
                          left outer join user_ipesp.tb_informacao_bancaria c
                              on      a.cod_ins          = c.cod_ins
                                  and a.cod_ide_cli      = c.cod_ide_cli
                                  and c.cod_ide_inf_banc = (select min(cod_ide_inf_banc)
                                                            from user_ipesp.tb_informacao_bancaria c_min
                                                            where c_min.cod_ins     = a.cod_ins
                                                              and c_min.cod_ide_cli = a.cod_ide_cli)
                          left outer join user_ipesp.tb_dependente d
                              on      a.cod_ins     = d.cod_ins
                                  and a.cod_ide_cli = d.cod_ide_cli_serv
                                  and d.cod_parentesco = '00010'  -- TB_CODIGO: COD_NUM = 2015, COD_PAR = '00010', DES_DESCRICAO = 'Conjuge'
                          left outer join user_ipesp.tb_pessoa_fisica e 
                              on      d.cod_ins          = e.cod_ins
                                  and d.cod_ide_cli_serv = e.cod_ide_cli
                          left outer join recenseamento.tb_pessoa_fisica f
                              on      a.cod_ins     = f.cod_ins
                                  and a.cod_ide_cli = f.cod_ide_cli
                       where a.cod_ins     = i_cod_ins
                         and a.cod_ide_cli = i_cod_ide_cli
                         /* Exige que o registro de tb_pessoa_fisica esteja regular em tb_beneficiario */
                         and exists (select 'x'
                                     from user_ipesp.tb_beneficiario g
                                     where g.cod_ins = i_cod_ins
                                       and g.cod_ide_cli_ben = i_cod_ide_cli
                                       and upper(g.flg_status) in ('A', 'S')
                                       and nvl(g.dat_fim_ben, (sysdate + 1)) > sysdate)
                       ) loop

                v_qtd_cid_nasc := 0;

                select count(*) 
                into v_qtd_cid_nasc
                from user_ipesp.tb_municipio
                where upper(nom_municipio) = upper(pf.des_natural);

                if (v_qtd_cid_nasc = 1) then
                    select cod_municipio
                    into v_cod_cid_nasc
                    from user_ipesp.tb_municipio
                    where upper(nom_municipio) = upper(pf.des_natural);
                else
                    v_cod_cid_nasc := null;
                end if;

                if (pf.cod_nacio = '1') then
                    v_cod_pais_nasc := '105';
                else
                    v_cod_pais_nasc := null;
                end if;

                if (pf.cod_ide_cli_recen is null) then -- registro nao existe em recenseamento.tb_pessoa_fisica

                    v_passo := 2;
                    v_id_recen := recenseamento.seq_essoa_fisica.nextval;

                    insert into recenseamento.tb_pessoa_fisica
                            (id,                     -- 1
                             cat_cnh,                -- 2
                             cod_cid_nasc,           -- 3
                             cod_cla_condtrab,       -- 4
                             cod_est_civ,            -- 5
                             cod_ide_cli,            -- 6
                             cod_ins,                -- 7
                             cod_nacio,              -- 8
                             cod_org_emi_rg,         -- 9
                             cod_pais_nasc,          -- 10
                             cod_raca,               -- 11
                             cod_sexo,               -- 12
                             cod_uf_cnh,             -- 13
                             cod_uf_emi_rg,          -- 14
                             cod_uf_nasc,            -- 15
                             dat_cheg_est,           -- 16
                             dat_criacao,            -- 17
                             dat_emi_rg,             -- 18
                             dat_exp_cnh,            -- 19
                             dat_exp_est,            -- 20
                             dat_first_cnh,          -- 21
                             dat_nasc,               -- 22
                             dat_nasc_conjuge,       -- 23
                             dat_obito,              -- 24
                             dat_recadastramento,    -- 25
                             dat_recenseamento,      -- 26
                             dat_ult_atu,            -- 27
                             dat_val_cnh,            -- 28
                             dv_agencia,             -- 29
                             dv_conta,               -- 30
                             email,                  -- 31
                             flag_recadastramento,   -- 32
                             flag_recenseamento,     -- 33
                             flag_uniao_estavel,     -- 34
                             flg_cas_bra,            -- 35
                             flg_def_auditiva,       -- 36
                             flg_def_fisica,         -- 37
                             flg_def_intelectual,    -- 38
                             flg_def_mental,         -- 39
                             flg_def_visual,         -- 40
                             flg_fil_bra,            -- 41
                             foto,                   -- 42
                             mom_mae,                -- 43
                             mom_pai,                -- 44
                             nom_cargo,              -- 45
                             nom_conjuge,            -- 46
                             nom_pessoa_fisica,      -- 47
                             nom_pro_criacao,        -- 48
                             nom_pro_ult_atu,        -- 49
                             nom_social,             -- 50
                             nom_usu_criacao,        -- 51
                             nom_usu_ult_atu,        -- 52
                             num_agencia,            -- 53
                             num_banco,              -- 54
                             num_cartorio_casa,      -- 55
                             num_cartorio_nasc,      -- 56
                             num_cer_res,            -- 57
                             num_cnh,                -- 58
                             num_conta,              -- 59
                             num_cpf,                -- 60
                             num_cpf_conjuge,        -- 61
                             num_folha_casa,         -- 62
                             num_folha_nasc,         -- 63
                             num_ins_reg_nac_est,    -- 64
                             num_livro_casa,         -- 65
                             num_livro_nasc,         -- 66
                             num_org_emi_est,        -- 67
                             num_rg,                 -- 68
                             num_sec_ele,            -- 69
                             num_til_ele,            -- 70
                             num_zon_ele,            -- 71
                             cod_reg_casamento,      -- 72
                             num_pis,                -- 73
                             num_nit,                -- 74
                             flg_elaborais,          -- 75
                             nom_cartorio,           -- 76
                             num_tel_fone1,          -- 77
                             num_tel_fone2,          -- 78
                             cod_tel_fone1,          -- 79
                             cod_tel_fone2)          -- 80
                        values 
                            (v_id_recen,                    -- 1 id                     
                             null,                          -- 2 cat_cnh             
                             v_cod_cid_nasc,                -- 3 cod_cid_nasc
                             null,                          -- 4 cod_cla_condtrab
                             pf.cod_est_civ,                -- 5 cod_est_civ        
                             pf.cod_ide_cli,                -- 6 cod_ide_cli     
                             pf.cod_ins,                    -- 7 cod_ins
                             pf.cod_nacio,                  -- 8 cod_nacio
                             pf.cod_org_emi_rg,             -- 9 cod_org_emi_rg
                             v_cod_pais_nasc,               -- 10 cod_pais_nasc
                             pf.cod_raca,                   -- 11 cod_raca
                             pf.cod_sexo,                   -- 12 cod_sexo
                             null,                          -- 13 cod_uf_cnh
                             pf.cod_uf_emi_rg,              -- 14 cod_uf_emi_rg
                             substr(pf.cod_uf_nasc, 1, 2),  -- 15 cod_uf_nasc
                             pf.dat_cheg_pais,              -- 16 dat_cheg_est
                             sysdate,                       -- 17 dat_criacao
                             pf.dat_emi_rg,                 -- 18 dat_emi_rg
                             null,                          -- 19 dat_exp_cnh
                             null,                          -- 20 dat_exp_est
                             null,                          -- 21 dat_first_cnh
                             pf.dat_nasc,                   -- 22 dat_nasc
                             pf.dat_nasc_conjuge,           -- 23 dat_nasc_conjuge
                             pf.dat_obito,                  -- 24 dat_obito
                             null,                          -- 25 dat_recadastramento
                             null,                          -- 26 dat_recenseamento
                             sysdate,                       -- 27 dat_ult_atu
                             null,                          -- 28 dat_val_cnh
                             pf.num_dv_agencia,             -- 29 dv_agencia
                             pf.num_dv_conta,               -- 30 dv_conta
                             pf.des_email,                  -- 31 email
                             null,                          -- 32 flag_recadastramento
                             null,                          -- 33 flag_recenseamento
                             nvl(pf.flg_uniao_estavel, 'N'), -- 34 flag_uniao_estavel
                             null,                          -- 35 flg_cas_bra
                             null,                          -- 36 flg_def_auditiva
                             null,                          -- 37 flg_def_fisica
                             null,                          -- 38 flg_def_intelectual
                             null,                          -- 39 flg_def_mental
                             null,                          -- 40 flg_def_visual
                             null,                          -- 41 flg_fil_bra
                             null,                          -- 42 foto
                             pf.nom_mae,                    -- 43 mom_mae
                             pf.nom_pai,                    -- 44 mom_pai
                             null,                          -- 45 nom_cargo
                             pf.nom_conjuge,                -- 46 nom_conjuge
                             pf.nom_pessoa_fisica,          -- 47 nom_pessoa_fisica
                             'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',     -- 48 nom_pro_criacao
                             'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',     -- 49 nom_pro_ult_atu
                             null,                          -- 50 nom_social
                             user,                          -- 51 nom_usu_criacao
                             user,                          -- 52 nom_usu_ult_atu
                             pf.num_agencia,                -- 53 num_agencia
                             pf.cod_banco,                  -- 54 num_banco
                             null,                          -- 55 num_cartorio_casa
                             pf.num_cartorio_nasc,          -- 56 num_cartorio_nasc
                             pf.num_cer_res,                -- 57 num_cer_res
                             null,                          -- 58 num_cnh
                             pf.num_conta,                  -- 59 num_conta
                             pf.num_cpf,                    -- 60 num_cpf
                             pf.num_cpf_conjuge,            -- 61 num_cpf_conjuge
                             null,                          -- 62 num_folha_casa
                             pf.num_folha_nasc,             -- 63 num_folha_nasc
                             null,                          -- 64 num_ins_reg_nac_est
                             null,                          -- 65 num_livro_casa
                             pf.num_livro_nasc,             -- 66 num_livro_nasc
                             null,                          -- 67 num_org_emi_est
                             pf.num_rg,                     -- 68 num_rg
                             pf.num_sec_ele,                -- 69 num_sec_ele
                             pf.num_tit_ele,                -- 70 num_til_ele
                             pf.num_zon_ele,                -- 71 num_zon_ele
                             pf.cod_reg_casamento,          -- 72 cod_reg_casamento
                             pf.num_pis,                    -- 73 num_pis
                             pf.num_nit_inss,               -- 74 num_nit
                             null,                          -- 75 flg_elaborais
                             null,                          -- 76 nom_cartorio
                             pf.num_tel_1,                  -- 77 num_tel_fone1
                             pf.num_tel_2,                  -- 78 num_tel_fone2
                             pf.tip_num_tel_1,              -- 79 cod_tel_fone1
                             pf.tip_num_tel_2);             -- 80 cod_tel_fone2

                else -- registro existe em recenseamento.tb_pessoa_fisica

                    v_id_recen := pf.id_recen;

                    if (     (nvl(pf.flag_recadastramento, 0) = 0)
                         and (pf.dat_recadastramento is null) 
                         and (nvl(pf.dat_ult_atu_recen, sysdate) < nvl(pf.dat_ult_atu, (sysdate + 1)) ) ) then

                        v_passo := 3;
                        -- nao foi efetuado o recadastramento e o registro de recenseamento e mais antigo que o de user_ipesp
                        update recenseamento.tb_pessoa_fisica
                        set cod_cid_nasc       = v_cod_cid_nasc,                -- 3            
                            cod_est_civ        = pf.cod_est_civ,                -- 5         
                            cod_nacio          = pf.cod_nacio,                  -- 8 
                            cod_org_emi_rg     =  pf.cod_org_emi_rg,            -- 9 
                            cod_pais_nasc      = v_cod_pais_nasc,               -- 10 
                            cod_raca           = pf.cod_raca,                   -- 11               
                            cod_sexo           = pf.cod_sexo,                   -- 12               
                            cod_uf_emi_rg      = pf.cod_uf_emi_rg,              -- 14          
                            cod_uf_nasc        = pf.cod_uf_nasc,                -- 15            
                            dat_cheg_est       = pf.dat_cheg_pais,              -- 16           
                            dat_emi_rg         = pf.dat_emi_rg,                 -- 18             
                            dat_nasc           = pf.dat_nasc,                   -- 22               
                            dat_nasc_conjuge   = pf.dat_nasc_conjuge,           -- 23       
                            dat_obito          = pf.dat_obito,                  -- 24              
                            dat_ult_atu        = sysdate,                       -- 27            
                            dv_agencia         = pf.num_dv_agencia,             -- 29             
                            dv_conta           = pf.num_dv_conta,               -- 30               
                            email              = pf.des_email,                  -- 31                  
                            flag_uniao_estavel = nvl(pf.flg_uniao_estavel, 'N'), -- 34     
                            mom_mae            = pf.nom_mae,                    -- 43                
                            mom_pai            = pf.nom_pai,                    -- 44                
                            nom_conjuge        = pf.nom_conjuge,                -- 46            
                            nom_pessoa_fisica  = pf.nom_pessoa_fisica,          -- 47      
                            nom_pro_ult_atu    = 'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST', -- 49        
                            nom_usu_ult_atu    = user,                          -- 52        
                            num_agencia        = pf.num_agencia,                -- 53            
                            num_banco          = pf.cod_banco,                  -- 54              
                            num_cartorio_nasc  = pf.num_cartorio_nasc,          -- 56      
                            num_cer_res        = pf.num_cer_res,                -- 57            
                            num_conta          = pf.num_conta,                  -- 59              
                            num_cpf            = pf.num_cpf,                    -- 60                
                            num_cpf_conjuge    = pf.num_cpf_conjuge,            -- 61        
                            num_folha_nasc     = pf.num_folha_nasc,             -- 63         
                            num_livro_nasc     = pf.num_livro_nasc,             -- 66         
                            num_rg             = pf.num_rg,                     -- 68                 
                            num_sec_ele        = pf.num_sec_ele,                -- 69            
                            num_til_ele        = pf.num_tit_ele,                -- 70            
                            num_zon_ele        = pf.num_zon_ele,                -- 71            
                            cod_reg_casamento  = pf.cod_reg_casamento,          -- 72      
                            num_pis            = pf.num_pis,                    -- 73                
                            num_nit            = pf.num_nit_inss,               -- 74                
                            num_tel_fone1      = pf.num_tel_1,                  -- 77          
                            num_tel_fone2      = pf.num_tel_2,                  -- 78 
                            cod_tel_fone1      = pf.tip_num_tel_1,              -- 79 
                            cod_tel_fone2      = pf.tip_num_tel_2               -- 80 
                        where id = pf.id_recen;

                    else

                        recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',
                                                        'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo,
                                                        'ja foi efetuado o recadastramento ou registro de'||
                                                            ' recenseamento.tb_pessoa_fisica nao e mais antigo'||
                                                            ' que o de user_ipesp.tb_pessoa_fisica'||
                                                            '(flag_recadastramento: '||nvl(to_char(pf.flag_recadastramento), '-nulo-')||
                                                            ', dat_recadastramento: '||nvl(to_char(pf.dat_recadastramento, 'dd/mm/yyyy'), '-nulo-')||
                                                            ', dat_ult_atu - recenseamento: '||to_char(nvl(pf.dat_ult_atu_recen, sysdate), 'dd/mm/yyyy')||
                                                            ', dat_ult_atu - user_ipesp: '||to_char(nvl(pf.dat_ult_atu, sysdate), 'dd/mm/yyyy')||')');

                    end if;

                end if;

                v_passo := 4;

                begin
                    v_usu_existe := 'FALSE';

                    select 'TRUE'
                    into v_usu_existe
                    from recenseamento.tb_usuario
                    where pessoa_fisica_id = v_id_recen
                      and rownum = 1;
                exception
                    when no_data_found then
                        null;
                    when others then
                        recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',
                                                        'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo,
                                                        'Erro ao pesquisar usuario: '||sqlerrm);
                    raise;
                end;

                if (v_usu_existe = 'FALSE') then -- usuario nao esta cadastrado

                    SP_REGISTRA_ACESSO_USUARIO (v_id_recen, nvl(pf.login, pf.num_cpf), v_msg);

                    if (v_msg is not null) then

                        recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',
                                                            'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo,
                                                            'SP_REGISTRA_ACESSO_USUARIO('||v_id_recen||', '||nvl(pf.login, pf.num_cpf)||'): '||v_msg);

                    end if;

                else    -- usuario ja cadastrado, nao altera nada

                    null;

                end if;

            end loop;

            v_passo := 7;

            for bf in (select a.cod_beneficio, a.cod_ide_cli_ben, a.cod_ins, a.dat_fim_ben, a.dat_ini_ben, a.flg_status,
                              a.num_prontuario, a.val_percentual, a.dat_ult_atu,
                              b.cod_entidade, b.cod_tipo_beneficio, b.val_percent_ben, b.dat_concessao,
                              c.val_percent_rateio,
                              d.cod_ide_cli cod_ide_cli_ben_recen, d.flag_recadastramento, d.dat_recadastramento, 
                              d.dat_ult_atu dat_ult_atu_recen, d.cod_beneficio cod_beneficio_recen, d.id id_recen,
                              case 
                                  when e.cod_fcrubrica like '405%' then 
                                      1
                                  else 
                                      0
                                  end flg_peculio,
                              case 
                                  when h.cod_fcrubrica like '406%' then 
                                      1
                                  else 
                                      0
                                  end flg_complemento_inss

                       from user_ipesp.tb_beneficiario a

                          inner join user_ipesp.tb_concessao_beneficio b
                              on      b.cod_ins       = a.cod_ins
                                  and b.cod_beneficio = a.cod_beneficio

                          left outer join user_ipesp.tb_hdet_calculado e
                              on      b.cod_ins          = e.cod_ins
                                  and b.cod_beneficio    = e.cod_beneficio
                                  --and b.cod_ide_cli_serv = e.cod_ide_cli
                                  and e.per_processo     = (select max(e2.per_processo)
                                                            from user_ipesp.tb_hdet_calculado e2 
                                                            where e2.cod_ins       = e.cod_ins
                                                              and e2.cod_ide_cli   = e.cod_ide_cli
                                                              and e2.cod_beneficio = e.cod_beneficio)
                                  and e.cod_fcrubrica in (select f1.cod_fcrubrica
                                                          from user_ipesp.tb_formula_calculo f1, 
                                                               user_ipesp.tb_rubricas g1 
                                                          where f1.cod_rubrica  = g1.cod_rubrica 
                                                            and f1.cod_ins      = b.cod_ins 
                                                            and f1.cod_entidade = b.cod_entidade
                                                            and g1.cod_ins      = f1.cod_ins
                                                            and g1.cod_conceito = 405) --peculio

                             left outer join user_ipesp.tb_hdet_calculado h
                              on      b.cod_ins          = h.cod_ins
                                  and b.cod_beneficio    = h.cod_beneficio
                                  --and b.cod_ide_cli_serv = h.cod_ide_cli
                                  and h.per_processo     = (select max(h2.per_processo)
                                                            from user_ipesp.tb_hdet_calculado h2 
                                                            where h2.cod_ins       = h.cod_ins
                                                              and h2.cod_ide_cli   = h.cod_ide_cli
                                                              and h2.cod_beneficio = h.cod_beneficio)
                                  and h.cod_fcrubrica in (select f2.cod_fcrubrica
                                                          from user_ipesp.tb_formula_calculo f2, 
                                                               user_ipesp.tb_rubricas g2 
                                                          where f2.cod_rubrica  = g2.cod_rubrica 
                                                            and f2.cod_ins      = b.cod_ins 
                                                            and f2.cod_entidade = b.cod_entidade
                                                            and g2.cod_ins      = f2.cod_ins
                                                            and g2.cod_conceito = 406) --complemento_inss                               

                          left outer join user_ipesp.tb_rateio_beneficio c
                              on      a.cod_ins         = c.cod_ins
                                  and a.cod_ide_cli_ben = c.cod_ide_cli_ben
                                  and a.cod_beneficio   = c.cod_beneficio
                                  and a.num_seq_benef   = c.num_seq_benef

                          left outer join recenseamento.tb_beneficiario d
                              on      a.cod_ins         = d.cod_ins
                                  and a.cod_ide_cli_ben = d.cod_ide_cli
                                  and a.cod_beneficio   = d.cod_beneficio

                       where a.cod_ins = i_cod_ins
                         and a.cod_ide_cli_ben = i_cod_ide_cli
                         and upper(a.flg_status) in ('A', 'S')
                         and nvl(a.dat_fim_ben, (sysdate + 1)) > sysdate
                       ) loop

                -- 
                if (bf.cod_ide_cli_ben_recen is null) then -- registro nao existe em recenseamento.tb_beneficiario

                    v_passo := 8;
                    insert into recenseamento.tb_beneficiario
                            (id,                    -- 1
                             cod_ide_cli,           -- 2
                             cod_tipo_beneficio,    -- 3
                             dat_fim_beneficio,     -- 4
                             dat_ini_beneficio,     -- 5
                             flg_status,            -- 6
                             num_prontuario,        -- 7
                             porc_beneficio,        -- 8
                             porc_rateio,           -- 9
                             cod_ins,               -- 10
                             cod_entidade,          -- 11
                             dat_criacao,           -- 12
                             nom_pro_criacao,       -- 13
                             nom_pro_ult_atu,       -- 14
                             nom_usu_criacao,       -- 15
                             nom_usu_ult_atu,       -- 16
                             dat_recadastramento,   -- 17
                             dat_recenseamento,     -- 18
                             dat_ult_atu,           -- 19
                             flag_recadastramento,  -- 20
                             flag_recenseamento,    -- 21
                             cod_beneficio,         -- 22
                             flg_complemento_inss,  -- 23
                             flg_peculio,           -- 24
                             dat_concessao)         -- 25
                        values
                            (recenseamento.seq_eneficiario.nextval, -- 1  id
                             bf.cod_ide_cli_ben,                -- 2  cod_ide_cli
                             --bf.cod_tipo_beneficio,             -- 3  cod_tipo_beneficio
                             DECODE(bf.cod_tipo_beneficio, 'V', '4', 
                                                           'G', '4',
                                                           'I', '3', 
                                                           'M', '1', 
                                                           'T', '6',  
                                                           'C', '2', 
                                                           'P', '5', 
                                                           bf.cod_tipo_beneficio), -- 3  cod_tipo_beneficio
                             bf.dat_fim_ben,                    -- 4  dat_fim_beneficio
                             bf.dat_ini_ben,                    -- 5  dat_ini_beneficio
                             --bf.flg_status,                     -- 6  flg_status
                             DECODE(bf.flg_status, 'V', '1', 
                                                   'A', '1', 
                                                   'X', '2', 
                                                   'E', '2',  
                                                   'S', '3', 
                                                   bf.flg_status), -- 6  flg_status
                             bf.num_prontuario,                 -- 7  num_prontuario
                             bf.val_percentual,                 -- 8  porc_beneficio
                             nvl(bf.val_percent_rateio, 100),   -- 9  porc_rateio
                             bf.cod_ins,                        -- 10 cod_ins
                             bf.cod_entidade,                   -- 11 cod_entidade
                             sysdate,                           -- 12 dat_criacao
                             'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',         -- 13 nom_pro_criacao
                             'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',         -- 14 nom_pro_ult_atu
                             user,                              -- 15 nom_usu_criacao
                             user,                              -- 16 nom_usu_ult_atu
                             sysdate,                           -- 17 dat_recadastramento
                             sysdate,                           -- 18 dat_recenseamento
                             sysdate,                           -- 19 dat_ult_atu
                             1,                                 -- 20 flag_recadastramento
                             1,                                 -- 21 flag_recenseamento
                             bf.cod_beneficio,                  -- 22 cod_beneficio
                             bf.flg_complemento_inss,           -- 23 flg_complemento_inss
                             bf.flg_peculio,                    -- 24 flg_peculio
                             bf.dat_concessao);                 -- 25 dat_concessao

                else -- registro existe em recenseamento.tb_beneficiario

                    if (     (nvl(bf.flag_recadastramento, 0) = 0)
                         and (bf.dat_recadastramento is null) 
                         and (nvl(bf.dat_ult_atu_recen, sysdate) < nvl(bf.dat_ult_atu, (sysdate + 1)) ) ) then

                        v_passo := 9;
                        -- nao foi efetuado o recadastramento e o registro de recenseamento e mais antigo que o de user_ipesp
                        update recenseamento.tb_beneficiario
                        --set cod_tipo_beneficio = bf.cod_tipo_beneficio,           -- 3  
                        set cod_tipo_beneficio   = DECODE(bf.cod_tipo_beneficio, 'V', '4', 
                                                                                 'I', '3', 
                                                                                 'M', '1', 
                                                                                 'T', '6',  
                                                                                 'C', '2', 
                                                                                 'P', '5', 
                                                                                 bf.cod_tipo_beneficio), -- 3 
                            dat_fim_beneficio    = bf.dat_fim_ben,                  -- 4  
                            dat_ini_beneficio    = bf.dat_ini_ben,                  -- 5  
                            --flg_status = bf.flg_status,                           -- 6  
                            flg_status           = DECODE(bf.flg_status, 'V', '1', 
                                                                         'A', '1', 
                                                                         'X', '2', 
                                                                         'E', '2',  
                                                                         'S', '3', 
                                                                         bf.flg_status), -- 6
                            num_prontuario       = bf.num_prontuario,               -- 7  
                            porc_beneficio       = bf.val_percentual,               -- 8  
                            porc_rateio          = nvl(bf.val_percent_rateio, 100), -- 9  
                            cod_entidade         = bf.cod_entidade,                 -- 11 
                            nom_pro_ult_atu      = 'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',    -- 14 
                            nom_usu_ult_atu      = user,                            -- 16 
                            dat_ult_atu          = sysdate,                         -- 19 
                            flg_complemento_inss = bf.flg_complemento_inss,         -- 23 
                            flg_peculio          = bf.flg_peculio,                  -- 24
                            dat_concessao        = bf.dat_concessao                 -- 25
                        where id = bf.id_recen;

                    else

                        recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',
                                                        'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo,
                                                        'ja foi efetuado o recadastramento ou registro de'||
                                                            ' recenseamento.tb_beneficiario nao e mais antigo'||
                                                            ' que o de user_ipesp.tb_beneficiario'||
                                                            '(flag_recadastramento: '||nvl(to_char(bf.flag_recadastramento), '-nulo-')||
                                                            ', dat_recadastramento: '||nvl(to_char(bf.dat_recadastramento, 'dd/mm/yyyy'), '-nulo-')||
                                                            ', dat_ult_atu - recenseamento: '||to_char(nvl(bf.dat_ult_atu_recen, sysdate), 'dd/mm/yyyy')||
                                                            ', dat_ult_atu - user_ipesp: '||to_char(nvl(bf.dat_ult_atu, sysdate), 'dd/mm/yyyy')||')');

                    end if;

                end if;

            end loop;

        end if;

    EXCEPTION
        WHEN OTHERS THEN    

            rollback to sp1;

            o_msg := substr('Erro processando COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo||': '||
                           sqlerrm, 1, 1024);
            recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',
                                             'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo,
                                              sqlerrm);

    END    SP_CARGA_DADOS_RECADAST;

    -----------------------------------------------------------------------------------------------

    PROCEDURE SP_REGISTRA_ACESSO_USUARIO (i_pessoa_fisica_id in recenseamento.tb_usuario.pessoa_fisica_id%type,
                                          i_username         in recenseamento.tb_usuario.username%type,
                                          o_msg              in out varchar2) IS

        v_id_usuario    recenseamento.tb_usuario.id%type;
        v_pswd          recenseamento.tb_usuario."password"%type := '$2a$10$6FbHf.UtrH5p./Mnmh.VLeSOl7E2HntXyqKpi3dWzZSPsFUpIQrVW';

    BEGIN

        if (i_pessoa_fisica_id is null) then

            o_msg := 'Identificac?o da pessoa fisica n?o foi fornecida. Impossivel fazer o cadastro do usuario.';

        elsif (i_username is null) then

            o_msg := 'Login nao esta preenchido. Impossivel fazer o cadastro do usuario';

        elsif (v_role_user_id is null) then

            o_msg := 'Identificac?o da permiss?o do usuario nao esta preenchida. Impossivel fazer o cadastro do usuario';

        else

            v_id_usuario := recenseamento.seq_suario.nextval;

            insert into recenseamento.tb_usuario
                (id,                    -- 1
                 account_expired,       -- 2
                 account_locked,        -- 3
                 active,                -- 4
                 dat_term_resp,         -- 5
                 enabled,               -- 6
                 flag_term_resp,        -- 7
                 "password",              -- 8
                 password_expired,      -- 9
                 pessoa_fisica_id,      -- 10
                 username,              -- 11
                 dat_final_pre_cad,     -- 12
                 flag_final_pre_cad)    -- 13
            values
                (v_id_usuario,          -- 1 id
                 0,                     -- 2 account_expired
                 0,                     -- 3 account_locked
                 1,                     -- 4 active
                 null,                  -- 5 dat_term_resp
                 1,                     -- 6 enabled
                 null,                  -- 7 flag_term_resp
                 v_pswd,                -- 8 password
                 0,                     -- 9 password_expired
                 i_pessoa_fisica_id,    -- 10 pessoa_fisica_id
                 i_username,            -- 11 username
                 null,                  -- 12 dat_final_pre_cad
                 null);                 -- 13 flag_final_pre_cad

            insert into recenseamento.tb_usuario_perfil
                (perfil_id,      usuario_id)
            values
                (v_role_user_id, v_id_usuario);

        end if;

    EXCEPTION
        WHEN OTHERS THEN    

            o_msg := sqlerrm;

    END    SP_REGISTRA_ACESSO_USUARIO;


    -----------------------------------------------------------------------------------------------

    FUNCTION FNC_PERM_CARGA_PESSOA_RECAD (i_cod_ins     in number,
                                          i_cod_ide_cli in varchar)
        RETURN VARCHAR2                                                                        
        IS

        function veri_dat_nasc_nula(i_cod_ins_vdnn in number,
                                    i_cod_ide_cli_vdnn in varchar) return varchar2 is

            v_dat_nasc user_ipesp.tb_pessoa_fisica.dat_nasc%type;
            v_retorno  varchar2(5) := 'TRUE';

        begin

            begin
                select dat_nasc
                into v_dat_nasc
                from user_ipesp.tb_pessoa_fisica
                where cod_ins     = i_cod_ins_vdnn
                  and cod_ide_cli = i_cod_ide_cli_vdnn;
            exception
                when others then
                    null;
            end;

            if ( v_dat_nasc is null) then

                recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                                  'COD_INS:'||i_cod_ins_vdnn||', COD_IDE_CLI:'||i_cod_ide_cli_vdnn,
                                                  'Data de nascimento nao esta preenchida');
                v_retorno := 'FALSE';

            end if;

            return (v_retorno);

        end;

        function veri_recad_pessoa_fisica(i_cod_ins_vrcp     in number,
                                          i_cod_ide_cli_vrcp in varchar) return varchar2 is

            v_flag_recadastramento recenseamento.tb_pessoa_fisica.flag_recadastramento%type;
            v_dat_recadastramento  recenseamento.tb_pessoa_fisica.dat_recadastramento%type;
            v_retorno              varchar2(5) := 'TRUE';

        begin

            begin
                select flag_recadastramento, dat_recadastramento
                into v_flag_recadastramento, v_dat_recadastramento
                from recenseamento.tb_pessoa_fisica
                where cod_ins     = i_cod_ins_vrcp
                  and cod_ide_cli = i_cod_ide_cli_vrcp;
            exception
                when others then
                    null;
            end;

            if (   (nvl(v_flag_recadastramento, 0) > 0) or (v_dat_recadastramento is not null) ) then

                recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                                  'COD_INS:'||i_cod_ins_vrcp||', COD_IDE_CLI:'||i_cod_ide_cli_vrcp,
                                                  'Recadastramento iniciado/realizado para pessoa fisica: flag_recadastramento='||
                                                        nvl(to_char(v_flag_recadastramento), '-nulo-') ||
                                                        ', dat_recadastramento='|| 
                                                        nvl(to_char(v_dat_recadastramento, 'dd/mm/yyyy hh24:mi'), '-nulo-') );
                v_retorno := 'FALSE';

            end if;

            return (v_retorno);

        end;

        function veri_recad_beneficiario(i_cod_ins_vrb in number,
                                         i_cod_ide_cli_vrb in varchar) return varchar2 is

            v_flag_recadastramento recenseamento.tb_beneficiario.flag_recadastramento%type;
            v_dat_recadastramento  recenseamento.tb_beneficiario.dat_recadastramento%type;
            v_retorno              varchar2(5) := 'TRUE';

        begin

            for bn in (select flag_recadastramento, dat_recadastramento
                       from recenseamento.tb_beneficiario
                       where cod_ins     = i_cod_ins_vrb
                         and cod_ide_cli = i_cod_ide_cli_vrb
                       ) loop 

                if ( (nvl(v_flag_recadastramento, 0) > 0) or (v_dat_recadastramento is not null) ) then
                    recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                                      'COD_INS:'||i_cod_ins_vrb||', COD_IDE_CLI:'||i_cod_ide_cli_vrb,
                                                      'Recadastramento iniciado/realizado para beneficiario: flag_recadastramento='||
                                                            nvl(to_char(v_flag_recadastramento), '-nulo-') ||
                                                            ', dat_recadastramento='|| 
                                                            nvl(to_char(v_dat_recadastramento, 'dd/mm/yyyy hh24:mi'), '-nulo-') );
                    v_retorno := 'FALSE';
                    exit;
                end if;

            end loop;

            return (v_retorno);

        end;

        function veri_beneficio_encerrado(i_cod_ins_vbe in number,
                                          i_cod_ide_cli_vbe in varchar) return varchar2 is

            v_ben_existe    boolean := false;
            v_ben_valido    boolean := false;
            v_retorno       varchar2(5) := 'TRUE';

        begin

            for bn in (select flg_status, dat_fim_ben
                       from user_ipesp.tb_beneficiario
                       where cod_ins         = i_cod_ins_vbe
                         and cod_ide_cli_ben = i_cod_ide_cli_vbe
                       ) loop 

                v_ben_existe := true;

                if ( ( bn.flg_status in ('A', 'S') ) and ( nvl(bn.dat_fim_ben, (sysdate + 1)) > sysdate ) ) then
                    v_ben_valido := true;
                    exit;
                end if;

            end loop;

            if (NOT v_ben_existe) then
                recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                                  'COD_INS:'||i_cod_ins_vbe||', COD_IDE_CLI:'||i_cod_ide_cli_vbe,
                                                  'Pessoa fisica nao cadastrada em TB_BENEFICIARIO');
                v_retorno := 'FALSE';

            elsif (NOT v_ben_valido) then
                recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                                  'COD_INS:'||i_cod_ins_vbe||', COD_IDE_CLI:'||i_cod_ide_cli_vbe,
                                                  'Status do beneficiario/data de fim do beneficio nao permitido para recenseamento');
                v_retorno := 'FALSE';

            end if;


            return (v_retorno);

        end;

    BEGIN

        if (   (i_cod_ins is null) 
            or (i_cod_ide_cli is null) 
            or (veri_recad_pessoa_fisica(i_cod_ins, i_cod_ide_cli) = 'FALSE') 
            or (veri_recad_beneficiario(i_cod_ins, i_cod_ide_cli) = 'FALSE') 
            or (veri_beneficio_encerrado(i_cod_ins, i_cod_ide_cli) = 'FALSE') 
            or (veri_dat_nasc_nula(i_cod_ins, i_cod_ide_cli) = 'FALSE')
            ) then

            return ('FALSE');

        end if;

        return ('TRUE');

    EXCEPTION
        WHEN OTHERS THEN    
            recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                              'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli,
                                              sqlerrm);
            return ('FALSE');

    END FNC_PERM_CARGA_PESSOA_RECAD;            
    
    PROCEDURE sp_atu_pf_dat_obito(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                  i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                  o_cod_erro   out number,
                                  o_msg_erro   out varchar2)
    AS
      v_cod_erro number;
      v_msg_erro varchar2(2000);
      v_dat_obito_recens date;
      v_dat_obito_novapr date;
      v_data_nvl date := to_date('07/02/1914','dd/mm/yyyy');
      v_id number;
      erro exception;
      v_flg_excluido char;
      
      /*declare
          a number;
          b varchar2(2000);
        begin   
          for r in (select * 
                      from tb_pessoa_fisica pf
                     where to_char(pf.dat_nasc,'mm') in ('01','02','03','04'))
          loop
            pac_recadast_novaprev.sp_atu_pf_dat_obito(r.cod_ins, r.cod_ide_cli,a,b);        
          end loop;                                                
        end;
         
select cod_ide_cli,
       num_cpf,
       nom_pessoa_fisica,
       to_char(dat_nasc,'dd/mm/yyyy')
  from tb_pessoa_fisica pf
where to_char(dat_nasc,'mm') in ('01','02','03','04','05','06')
  and exists (select '1' 
                from user_ipesp.tb_pessoa_fisica pf2
               where pf.cod_ide_cli = pf2.cod_ide_cli
                 and exists( select '1' 
                               from user_ipesp.tb_beneficiario b 
                              where b.cod_ide_cli_ben = pf2.cod_ide_cli
                                and b.dat_ini_ben >= trunc(sysdate,'yyyy')
                                and b.dat_fim_ben is null 
                                and b.flg_status in ('A','V','S'))
                 and not exists( select '1' 
                               from user_ipesp.tb_beneficiario b 
                              where b.cod_ide_cli_ben = pf2.cod_ide_cli
                                and b.dat_ini_ben < trunc(sysdate,'yyyy'))                                                                
                                )
   order by nom_pessoa_fisica ;
        
        
        */
      
                                           
    BEGIN
      -- Verificando se pessoa fisica existe
      begin 
        select pf.dat_obito, pf.id, pf.flg_excluido
          into v_dat_obito_recens, v_id, v_flg_excluido
          from tb_pessoa_fisica pf
         where pf.cod_ins = i_cod_ins
           and pf.cod_ide_cli = i_cod_ide_cli;   
      exception 
        when no_data_found then 
           v_cod_erro := 1;
           v_msg_erro := 'N?o localizada pessoa fisica '||i_cod_ide_cli||' na base do recenseamento';
           raise erro;
      end;
      --
      -- Se registro ja estiver excluido, n?o tem necessidade de fazer nada. 
      if nvl(v_flg_excluido,'N') = 'S' then 
        return;
      end if;
      --
      -- Verifica se existe data obito na novaprev
      begin 
        select pff.dat_obito
          into v_dat_obito_novapr
          from useR_ipesp.Tb_Pessoa_Fisica pff
         where pff.cod_ins = i_cod_ins
           and pff.cod_ide_cli = i_cod_ide_cli;
      exception 
        when no_data_found then 
          v_dat_obito_novapr := null;
          v_msg_erro := 'N?o localizada pessoa fisica '||i_cod_ide_cli||' na base da novaprev';
          raise erro;
      end;
      --
      --
      if v_dat_obito_recens is null and v_dat_obito_novapr is not null then 
        update tb_pessoa_fisica
           set dat_obito = v_dat_obito_novapr,
               flg_excluido = 'S',
               des_mot_excluido = 'Registro excluido por Obito no sistema da Novaprev.' ,
               dat_ult_atu = sysdate,
               nom_pro_ult_atu = 'sp_atu_pf_dat_obito'
         where id = v_id;
         -- apaga agendamento do cliente
         sp_remove_agendamento_pf(v_id,v_cod_erro,v_msg_erro);
         if v_cod_erro is not null then
           raise erro;
         end if;
      end if;
    exception 
      when erro then 
        o_cod_erro := v_cod_erro;
        o_msg_erro := '[PAC_RECADAST_NOVAPREV.SP_ATUALIZA_DAT_OBITO] - Erro Interno '||v_cod_erro||' - '||v_msg_erro;
      when others then 
        o_cod_erro := 0;
        o_msg_erro := '[PAC_RECADAST_NOVAPREV.SP_ATUALIZA_DAT_OBITO] - Erro Geral - '||sqlerrm;
    END sp_atu_pf_dat_obito;

    -----------------------------------------------------------------------------------------------
    
    function fnc_ret_rel_ben_pendente (i_nom_pessoa_fisica in varchar2,
                                       i_num_cpf           in varchar2,
                                       i_num_matricula     in varchar2, 
                                       i_mes_aniversario   in varchar2) return typ_tab_ben_pendente pipelined
    as
      v_rec_ben_pendente typ_rec_ben_pendente;
      
    begin
      for r in (
      SELECT DISTINCT P1.NUM_PRONTUARIO AS NUM_PRONTUARIO,
                P1.NOM_PESSOA_FISICA AS NOM_BENEFICIARIO,
                P1.CPF AS NUM_CPF,
                P1.TIPO_BENEFICIO AS TIPO_BENEFICIO,
                P1.DAT_CONCESSAO AS DAT_CONCESSAO,
                P1.TIP_LOGRADOURO AS TIP_LOGRADOURO,
                P1.LOGRADOURO AS LOGRADOURO,
                P1.NUMERO AS NUMERO,
                P1.BAIRRO AS BAIRRO,
                P1.MUNICIPIO AS MUNICIPIO,
                P1.CEP AS CEP,
                P1.ESTADO AS ESTADO,
                FNC_FORMATA_TELEFONE(P1.NUM_TEL_FONE1) AS TELEFONE,
                P1.DAT_NASC AS DAT_NASC,
                (CASE WHEN P1.flg_atend_ini_nao_finalizado = 'S' THEN '4'
                      WHEN P1.flg_agendou = 'S' AND P1.flg_comp_dia_agen = 'N' THEN '3'
                      WHEN P1.flg_agendou = 'S' THEN '2' 
                      ELSE '1' END) AS FLG_SIT_RECAD,
                P2.NAME AS DES_SIT_RECAD
        FROM (SELECT DISTINCT pf.id AS ID,
                              PF.COD_IDE_CLI AS COD_IDE_CLI,
                              PF.NUM_TEL_FONE1 AS NUM_TEL_FONE1,
                              CAST(PF.dat_nasc AS DATE) AS NASC,
                              pf.nom_pessoa_fisica AS NOM_PESSOA_FISICA,
                              pf.num_cpf AS CPF,
                              BN.NUM_PRONTUARIO AS NUM_PRONTUARIO,
                              TB.name AS TIPO_BENEFICIO,
                              CAST(BN.dat_concessao AS DATE) as dat_concessao,
                              T1.NOM_TIPO AS TIP_LOGRADOURO,
                              E1.NOM_LOGRADOURO AS LOGRADOURO,
                              E1.NUM_NUMERO AS NUMERO,
                              B1.NOM_BAIRRO AS BAIRRO,
                              M1.NOM_ABREVIADO AS MUNICIPIO,
                              E1.NUM_CEP AS CEP,
                              E2.COD_DOMAIN_DETAIL AS ESTADO,
                              PF.DAT_NASC AS DAT_NASC,
                              nvl((SELECT 'S' FROM tb_agendamento ag1 WHERE ag1.pessoa_fisica_id = pf.id),'N') AS flg_agendou,
                              nvl((SELECT 'S' FROM tb_agendamento ag1 
                                    WHERE ag1.pessoa_fisica_id = pf.id 
                                      AND ag1.dat_agendamento < SYSDATE
                                      AND EXISTS (SELECT 1
                                                    FROM tb_atendimento at1
                                                   WHERE at1.servidor_id = pf.id
                                                     AND TO_DATE(TO_CHAR(at1.dat_ini_atend,'DD/MM/YYYY') || '00:00','DD/MM/YYYY HH24:MI') =
                                                         TO_DATE(TO_CHAR(ag1.dat_agendamento,'DD/MM/YYYY') ||'00:00','DD/MM/YYYY HH24:MI'))
                                   ),'N') AS flg_comp_dia_agen,
                              nvl((SELECT DISTINCT 'S'
                                    FROM tb_atendimento at1
                                   WHERE at1.servidor_id = pf.id
                                     AND NOT EXISTS (SELECT 1
                                                       FROM tb_protocolo pr
                                                      WHERE pr.cod_ide_cli = pf.cod_ide_cli)
                                   ),'N') AS flg_atend_ini_nao_finalizado
                FROM TB_PESSOA_FISICA PF
                  LEFT JOIN TB_END_PESSOA_FISICA E1
                    ON (PF.COD_IDE_CLI = E1.COD_IDE_CLI AND E1.COD_TIPO_END = 601)
                  LEFT JOIN TB_END_TIPO_LOGRADOURO T1
                    ON (T1.ID = E1.COD_TIP_LOGRADOURO)
                  LEFT JOIN TB_END_BAIRRO B1
                    ON (E1.COD_BAIRRO = B1.ID)
                  LEFT JOIN TB_END_LOCALIDADE M1
                    ON (E1.COD_MUNICIPIO = M1.ID)
                  LEFT JOIN TB_DOMINIO_DETALHE E2
                    ON (E2.DOMAIN_ID = 3 AND E2.ID = E1.COD_UF)
                  LEFT JOIN tb_usuario us
                    ON us.pessoa_fisica_id = pf.id
                  LEFT JOIN tb_usuario_perfil up
                    ON up.usuario_id = us.id
                  LEFT JOIN tb_perfil pe
                    ON pe.id = up.perfil_id
                  LEFT JOIN tb_beneficiario bn
                    ON bn.cod_ide_cli = pf.cod_ide_cli
                  LEFT JOIN tb_dominio_detalhe tb
                    ON tb.domain_id = (SELECT id FROM tb_dominio WHERE cod_domain = 'TIPO_BENEFICIO')
                    AND tb.cod_domain_detail = bn.cod_tipo_beneficio        
                  LEFT JOIN tb_protocolo pr
                    ON pr.cod_ide_cli = pf.cod_ide_cli
               WHERE pe.authority = 'ROLE_USER'
                 AND PF.Dat_Obito is null
                 AND pr.id IS NULL
                 AND ((i_nom_pessoa_fisica is not null AND upper(pf.nom_pessoa_fisica) LIKE upper('%' || i_nom_pessoa_fisica || '%')) OR (i_nom_pessoa_fisica is null))
                 AND ((i_num_cpf is not null AND pf.num_cpf LIKE upper('%' || i_num_cpf || '%')) OR (i_num_cpf is null ))
                 AND ((i_num_matricula is not null AND bn.num_prontuario LIKE upper('%' || i_num_matricula || '%')) OR (i_num_matricula is null AND 1 = 1))
                 AND ((i_mes_aniversario is not null AND TO_CHAR(pf.dat_nasc, 'MM') LIKE upper('%' || i_mes_aniversario || '%')) OR (i_mes_aniversario is null AND 1 = 1))
               ORDER BY pf.nom_pessoa_fisica
             ) P1
        LEFT JOIN tb_dominio_detalhe P2 ON P2.domain_id = (SELECT id FROM tb_dominio WHERE cod_domain = 'SITUACAO_RECADASTRAMENTO')
                                        AND P2.cod_domain_detail = (CASE WHEN P1.flg_atend_ini_nao_finalizado = 'S' THEN '4'
                                                                         WHEN P1.flg_agendou = 'S' AND P1.flg_comp_dia_agen = 'N' THEN '3'
                                                                         WHEN P1.flg_agendou = 'S' THEN '2'
                                                                        ELSE '1' END)
      ) 
      loop         
        begin                
         dbms_output.put_line('W'||r.num_prontuario||'E');
          v_rec_ben_pendente.num_prontuario   :=  r.num_prontuario;
          v_rec_ben_pendente.nom_beneficiario :=  r.nom_beneficiario;
          v_rec_ben_pendente.num_cpf          :=  r.num_cpf;
          v_rec_ben_pendente.tipo_beneficio   :=  r.tipo_beneficio;
          v_rec_ben_pendente.dat_concessao    :=  r.dat_concessao; 
          v_rec_ben_pendente.tip_logradouro   :=  r.tip_logradouro;
          v_rec_ben_pendente.logradouro       :=  r.logradouro;    
          v_rec_ben_pendente.numero           :=  r.numero;        
          v_rec_ben_pendente.bairro           :=  r.bairro;        
          v_rec_ben_pendente.municipio        :=  r.municipio;     
          v_rec_ben_pendente.cep              :=  r.cep;           
          v_rec_ben_pendente.estado           :=  r.estado;        
          v_rec_ben_pendente.telefone         :=  r.telefone;      
          v_rec_ben_pendente.dat_nasc         :=  r.dat_nasc;      
          v_rec_ben_pendente.flg_sit_recad    :=  r.flg_sit_recad; 
          v_rec_ben_pendente.des_sit_recad    :=  r.des_sit_recad; 
          
          pipe row(v_rec_ben_pendente);
        exception 
          when others then 
            dbms_output.put_line(r.num_cpf);
            raise_application_error(-20000,sqlerrm);
        end;
       
      end loop; 
 
       
      
    end fnc_ret_rel_ben_pendente;     
    --
    PROCEDURE sp_atu_pf_ben_2019(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                    i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                    o_cod_erro   out number,
                                    o_msg_erro   out varchar2)
    as
      v_id number;
      v_qtd_ben number;
      v_flg_excluido char;
    begin
      --
      select count(1)
        into v_qtd_ben
        from user_ipesp.tb_pessoa_fisica pf
       where pf.cod_ide_cli = i_cod_ide_cli
         and exists( select '1' 
                       from user_ipesp.tb_beneficiario b 
                      where b.cod_ide_cli_ben = pf.cod_ide_cli
                        and b.dat_ini_ben >= trunc(sysdate,'yyyy')
                        and b.dat_fim_ben is null 
                        and b.flg_status in ('A','V','S'))
         and not exists( select '1' 
                       from user_ipesp.tb_beneficiario b 
                      where b.cod_ide_cli_ben = pf.cod_ide_cli
                        and b.dat_ini_ben < trunc(sysdate,'yyyy'));                                                                                        
      --                            
      if v_qtd_ben > 0 then 
        --
        select id, flg_excluido
          into v_id, v_flg_excluido
          from tb_pessoa_fisica 
         where cod_ins = i_cod_ins
           and cod_ide_cli = i_cod_ide_cli;  
        --
        if nvl(v_flg_excluido,'N') <> 'S' then 
          update tb_pessoa_fisica pf
             set pf.flg_excluido = 'S',
                 pf.des_mot_excluido = 'Beneficio iniciou apos 2019 na novaprev para o servidor.',
                 dat_ult_atu = sysdate,
                 nom_pro_ult_atu = 'sp_atu_pf_ben_2019'
           where pf.cod_ins = i_cod_ins
             and pf.cod_ide_cli = i_cod_ide_cli;                 
          --
          sp_remove_agendamento_pf(v_id,o_cod_erro, o_msg_erro);
          --
        end if;
      end if;                                    
    end sp_atu_pf_ben_2019;
    
      
     PROCEDURE sp_atu_pf_ben_cessado(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                    i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                    o_cod_erro   out number,
                                    o_msg_erro   out varchar2)
    as
      v_id number;
      v_qtd_ben number;
      v_flg_excluido char;
    begin
      --
      select count(1)
        into v_qtd_ben
        from user_ipesp.tb_beneficiario ben 
       where ben.cod_ins = i_cod_ins
         and ben.cod_ide_cli_ben = i_cod_ide_cli
         and ben.flg_status not in ('V','A','S')
         and not exists (select '1'
                           from user_ipesp.tb_beneficiario ben
                          where ben.cod_ins = i_cod_ins
                            and ben.cod_ide_cli_ben = i_cod_ide_cli
                            and ben.flg_status in ('V','A','S'));                                                                                                
      --                            
      if v_qtd_ben > 0 then 
        --
        select id, flg_excluido
          into v_id, v_flg_excluido
          from tb_pessoa_fisica 
         where cod_ins = i_cod_ins
           and cod_ide_cli = i_cod_ide_cli;  
        --
        if nvl(v_flg_excluido,'N') <> 'S' then 
          update tb_pessoa_fisica pf
             set pf.flg_excluido = 'S',
                 pf.des_mot_excluido = 'Beneficio cessado na novaprev para o servidor.',
                 dat_ult_atu = sysdate,
                 nom_pro_ult_atu = 'sp_atu_pf_ben_cessado'
           where pf.cod_ins = i_cod_ins
             and pf.cod_ide_cli = i_cod_ide_cli;                 
          --
          sp_remove_agendamento_pf(v_id,o_cod_erro, o_msg_erro);
          --
        end if;
      end if;                                    
    end sp_atu_pf_ben_cessado;                                                                           
    --
    
    --
    -- Rotina que remove  todos os agendamentos de uma pessoa fisica
    procedure sp_remove_agendamento_pf(i_id_pf    in  tb_pessoa_fisica.id%type,
                                       o_cod_erro out number,
                                       o_msg_erro out varchar2)
    as      
    begin 
      insert into tb_agendamento_historico
      select id,
             cod_entidade,
             dat_agendamento,
             dat_ger_prot,
             hora_final,
             hora_inicio,
             local_id,
             pessoa_fisica_id,
             regiao_id,
             tipo,
             motivo_alteracao,
             flg_recadastramento,
             dat_recadastramento,
             flg_recenseamento,
             dat_recenseamento,
             sysdate,
             sysdate,
             'pac_recadast_novaprev.sp_remove_agendamento',
             'pac_recadast_novaprev.sp_remove_agendamento',
             'DELETE'
      from recenseamento.tb_agendamento a
      where a.pessoa_fisica_id = i_id_pf
        and a.dat_agendamento >= sysdate;      
      --
      delete tb_agendamento a
       where a.pessoa_fisica_id = i_id_pf
         and a.dat_agendamento >= sysdate;
      --  
    exception 
      when others then 
        o_cod_erro := 1;                            
        o_msg_erro := '[PAC_RECADAST_NOVAPREV.SP_REMOVE_AGENDAMENTO_PF] - '||sqlerrm;
    end sp_remove_agendamento_pf;
END PAC_RECADAST_NOVAPREV_2;
/

