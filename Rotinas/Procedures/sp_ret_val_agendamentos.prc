create or replace procedure sp_ret_val_agendamentos(i_dat_ini in date,
                                                    i_dat_fim in date,
                                                    i_flg_atend in char, -- S/N
                                                    i_flg_coord in char, -- S/N
                                                    o_qtd_ocupado out number,
                                                    o_qtd_livre out number,
                                                    o_pct_ocupado out number,
                                                    o_pct_livre out number)
as        

  v_qtd_agend_atend number;
  v_qtd_agend_coord number;
  v_num_tot_atend_adm number;
  v_num_tot_atend_adm_coord number;
  v_qtd_total number :=0;
  v_qtd_ocupado number :=0;
  
  v_dat_ini date;
  v_dat_fim date;
begin

o_qtd_ocupado := 60;
o_qtd_livre   := 140;
o_pct_ocupado := 29.5;
o_pct_livre   := 70.5;

  -- Seta valores de intervalo de datas. E trata caso as mesmas sejam nulas
  v_dat_ini := nvl(i_dat_ini, to_date('01/01/1900','dd/mm/yyyy'));
  v_dat_fim := nvl(i_dat_fim, to_date('01/01/2999','dd/mm/yyyy'));  

  -- Pega valores de agendamentos realizados por atendente 
  SELECT COUNT(*)
    into v_qtd_agend_atend
    FROM tb_agendamento a1
   WHERE a1.dat_agendamento BETWEEN v_dat_ini AND v_dat_fim;
   
  -- Pega valores de agendamentos realizados por coordenadore 
  SELECT COUNT(*)
    into v_qtd_agend_coord
    FROM tb_agendamento_pendentes a1
   WHERE a1.dat_agendamento BETWEEN v_dat_ini AND v_dat_fim;   
   
  -- Pega valores de agendamento total de atendente
  select sum(floor((minutos_per1+minutos_per2)/num_min_atd)* num_atds * num_dias) as atd_total
  into v_num_tot_atend_adm
  from (select ((per1_fim-per1_ini)*60*24) as minutos_per1,
               ((per2_fim-per2_ini)*60*24) as minutos_per2,
               num_min_atd,
               num_atds,
               least(dat_fim,v_dat_fim) - greatest(dat_ini,v_dat_ini) num_dias 
          from(select to_date('01/01/2000 '||per1_ini_atd,'dd/mm/yyyy hh24:mi') as per1_ini,
                      to_date('01/01/2000 '||per1_fim_atd,'dd/mm/yyyy hh24:mi') as per1_fim,
                      to_date('01/01/2000 '||per2_ini_atd,'dd/mm/yyyy hh24:mi') as per2_ini,
                      to_date('01/01/2000 '||per2_fim_atd,'dd/mm/yyyy hh24:mi') as per2_fim,
                      a.num_min_atd,
                      a.num_atds,
                      a.dat_ini,
                      a.dat_fim
                from tb_agendamento_adm a
                where a.dat_ini < v_dat_fim 
                  and  a.dat_fim > v_dat_ini
               ) 
      );
      
  select sum(floor((minutos_per1+minutos_per2)/num_min_atd)* num_atds * num_dias) as atd_total
  into v_num_tot_atend_adm_coord
  from (select ((per1_fim-per1_ini)*60*24) as minutos_per1,
               ((per2_fim-per2_ini)*60*24) as minutos_per2,
               num_min_atd,
               num_atds,
               least(dat_fim,v_dat_fim) - greatest(dat_ini,v_dat_ini) num_dias 
          from(select to_date('01/01/2000 '||per1_ini_atd,'dd/mm/yyyy hh24:mi') as per1_ini,
                      to_date('01/01/2000 '||per1_fim_atd,'dd/mm/yyyy hh24:mi') as per1_fim,
                      to_date('01/01/2000 '||per2_ini_atd,'dd/mm/yyyy hh24:mi') as per2_ini,
                      to_date('01/01/2000 '||per2_fim_atd,'dd/mm/yyyy hh24:mi') as per2_fim,
                      a.num_min_atd,
                      a.num_atds,
                      a.dat_ini,
                      a.dat_fim
                from tb_agendamento_coord a
                where a.dat_ini < v_dat_fim 
                  and  a.dat_fim > v_dat_ini
               ) 
      );

  if i_flg_atend = 'S' then 
    v_qtd_total := v_qtd_total + v_num_tot_atend_adm;
    v_qtd_ocupado := v_qtd_ocupado + v_qtd_agend_atend;
  end if;
  
  if i_flg_coord = 'S' then 
    v_qtd_total := v_qtd_total + v_num_tot_atend_adm_coord;
    v_qtd_ocupado := v_qtd_ocupado + v_qtd_agend_coord;
  end if;    
  --
  o_qtd_ocupado := v_qtd_ocupado;
  o_qtd_livre   := v_qtd_total - v_qtd_ocupado;
  --
  -- calcula porcentagem
  o_pct_ocupado :=  round(((v_qtd_ocupado * 100)/v_qtd_total),3);
  o_pct_livre   :=  round((100 - o_pct_ocupado),3);            

end sp_ret_val_agendamentos;
/

