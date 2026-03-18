%macro gb_challenge_macro(
    m_train=,
    m_oot=,
    m_target=,
    m_time=,
    m_xb_pd=,
    m_num_inputs=,
    m_cat_inputs=,
    m_gb_stagnation=0,
    m_top_k=40,
    m_caslib=casuser,
    m_session=casauto,
    m_troncal=,
    m_segmento=,
    m_var_seg=,
    m_model_type=
);
    /* Iniciar sesión CAS (CASUSER global) */
    proc cas;
        session &m_session.;
        libname &m_caslib. cas caslib=&m_caslib.;
        options casdatalimit=ALL;
    quit;
    /* dropear todas las tablas promovidas en el caslib */
    %include "&_root_path/Sources/Macros/_drop_caslib.sas";
    %_drop_caslib(
        caslib_name =&m_caslib.,
        del_prom_tables = Y,
        cas_sess_name =&m_session.,
        terminate_session = N
    );
    /* exportar tabla train del work al caslib paa poder ser usada en el sampling*/
    %include "&_root_path/Sources/Macros/_promote_table.sas";
    %_promote_table(
        libname_input=work,
        libname_output=&m_caslib.,
        table_input=&m_train.,
        promote_flag=0
    );
    /* hacer el presampling si aplica al caso para el train */
    %include "&_root_path/Sources/Macros/_sampling_prechallenge.sas";
    %_sampling_prechallenge(
        libname_input=&m_caslib.,
        libname_output=&m_caslib.,
        data_input=&m_train.,
        target_input=&m_target.,
        time_input=&m_time.,
        seed=12345
    );
    /* Promover tabla train dentro del caslib */
    %_promote_table(
        libname_input=&m_caslib.,
        libname_output=&m_caslib.,
        table_input=&m_train.,
        promote_flag=1
    );
    /* exportar tabla oot del work al caslib */
    %_promote_table(
        libname_input=work,
        libname_output=&m_caslib.,
        table_input=&m_oot.,
        promote_flag=1
    );

    %include "&_root_path/Sources/Macros/_set_partition.sas";
    %_set_partition(
        train_input=&m_train.,
        oot_input=&m_oot.,
        target_input=&m_target.,
        time_input=&m_time.,
        libname_input=&m_caslib.,
        libname_output=&m_caslib.
    );

    %include "&_root_path/Sources/Macros/_calculate_gini.sas";
    %_calculate_gini(
        caslib_name=&m_caslib.,
        train_input =&m_train.,
        oot_input   =&m_oot.,
        target_input=&m_target.,
        xb_pd_input =&m_xb_pd.,
        ml_algo   = Gradient Boosting
    );

    %include "&_root_path/Sources/Macros/_get_gini_mensual.sas";
    %_get_gini_mensual(&m_caslib., full_data, &m_xb_pd., &m_target., 0, byvarl = &m_time.);

    %include "&_root_path/Sources/Modulos/m_gradient_boosting/__gb_set_ranges.sas";
    %__gb_set_ranges(caslib_name=&m_caslib., tabla=full_data, num=&m_num_inputs., cat=&m_cat_inputs., ntrees_cap=800);

    %include "&_root_path/Sources/Modulos/m_gradient_boosting/__gb_tune.sas";
    %__gb_tune(
        caslib_name=&m_caslib.,
        tabla=train_part,
        num_input=&m_num_inputs.,
        cat_input=&m_cat_inputs.,
        target_input=&m_target.,
        gb_stagnation=&m_gb_stagnation.
    );
    /* Selección de los mejores modelos */
    proc sort data=evaluationhistory out=hist_sorted; by descending GiniCoefficient; run;
    data best_cfg; set hist_sorted(obs=&m_top_k.); cfg_id=_n_; run;
    
    /* Crear tablas para almacenar los mejores resultados */
    proc sql;
        create table BEST_GB_RESULTS (
            cfg_id num,
            %if &m_gb_stagnation. eq 0 %then %do; NTREE num, %end;
            M num,
            LEARNINGRATE num format=best32.,
            SUBSAMPLERATE num format=best32.,
            LASSO num format=best32.,
            RIDGE num format=best32.,
            NBINS num,
            MAXLEVEL num,
            LEAFSIZE num,
            gini_train num,
            gini_oot num
        );
    quit;

    /* ==================================== */
    /*   CREAR LA PARTE DE PARALELIZACION   */
    /* ==================================== */

    %global gb_data_path group_act_process is_top_flg tr_sess seg_sess global_bmk_path;
    %let num_sessions = 5;
    %let tr_sess = &m_troncal.;
    %let seg_sess = &m_segmento.;
    %let gb_data_path = &_root_path./Troncal_&tr_sess./Data;
    %let global_bmk_path = &_root_path./Troncal_&tr_sess./Models/Benchmark;

    /* Dividir la tabla de mejores configuraciones en sesiones */
    data %do group_act_process=1 %to &num_sessions.; &m_caslib..best_cfg_session_&group_act_process.(copies=0 promote=yes) %end;;
        set best_cfg;
        session_id = mod(_N_ - 1, &num_sessions.) + 1;
        if session_id = 1 then output &m_caslib..best_cfg_session_1;
        %do group_act_process=2 %to &num_sessions.;
            else if session_id = &group_act_process. then output &m_caslib..best_cfg_session_&group_act_process.;
        %end;
    run;
    /* Iniciar las (signon) sesiones paralelas */
    %do group_act_process=1 %to &num_sessions.;
        signon task_&group_act_process. sascmd="!sascmd -nosyntaxcheck -noterminal";
        %syslput _global_/like='*' remote=task_&group_act_process.;
        %syslput _local_/like='*' remote=task_&group_act_process.;
    %end;
    /* Ejecutar las tareas en paralelo cada una en una sesion */
    %do group_act_process=1 %to &num_sessions.;
        rsubmit task_&group_act_process. wait=no;
        options MSGLEVEL=I NOFULLSTIMER formchar = '|----|+|---';
		options OBS=MAX NOSYNTAXCHECK REPLACE NOQUOTELENMAX;
            %include "&_root_path/Sources/Macros/LogsConfiguration.sas";
            %if &seg = 0 %then %do;
                %config_log(m_gb_task_&group_act_process., &&_logs_tcl_&tr);
            %end;
            %else %do;
                %config_log(m_gb_task_&group_act_process., &&_logs_tcl_&tr._seg_&seg);
            %end;
            cas casr&group_act_process. sessopts=(caslib="&m_caslib");
            libname &m_caslib. cas caslib=&m_caslib.;
            options casdatalimit=ALL;
            %include "&_root_path/Sources/Macros/_gini.sas";
            %include "&_root_path/Sources/Macros/_get_gini_mensual.sas";
            %include "&_root_path/Sources/Modulos/m_gradient_boosting/__gb_train.sas";
            %include "&_root_path/Sources/Macros/_create_caslib.sas";
            %include "&_root_path/Sources/Macros/_drop_caslib.sas";            
            %__gb_train(0);
            cas casr&group_act_process. terminate;
            %Config_Log_Restore;
        endrsubmit;
    %end;
    /* Esperar a que todas las tareas terminen */
    waitfor _ALL_ %do group_act_process = 1 %to &num_sessions.; 
        task_&group_act_process. %end;;
    /* Cerrar sesiones paralelas */
    %do group_act_process = 1 %to &num_sessions.;
        signoff task_&group_act_process.;
    %end;

    data BEST_GB_RESULTS;
        set %do group_act_process=1 %to &num_sessions.; &m_caslib..GB_RESULTS_SESSION_&group_act_process. %end;;
    run;
    proc sort data=best_gb_results out=top_5_gb_models; by descending gini_penalizado; run;
    data &m_caslib..top_5_gb_models(copies=0 promote=yes); set top_5_gb_models(obs=5); cfg_id = _n_; run;
    
    %include "&_root_path/Sources/Macros/_drop_caslib_table.sas";
    %_drop_caslib_table(
        m_cas_session=&m_session.,
        m_caslib=&m_caslib.,
        m_castable_name=best_cfg_session,
        is_loop=1
    );
    %_drop_caslib_table(
        m_cas_session=&m_session.,
        m_caslib=&m_caslib.,
        m_castable_name=gb_results_session,
        is_loop=1
    );

    %global gb_var_seg;
    %if %sysevalf(%superq(m_var_seg)=,boolean) %then %do;
        %let gb_var_seg = UNIVERSE;
    %end;
    %else %do;
        proc sql noprint;
            select distinct &m_var_seg. into :gb_var_seg from &m_caslib..full_data;
        quit;
    %end;

    data %do group_act_process=1 %to &num_sessions.; &m_caslib..best_cfg_session_&group_act_process(copies=0 promote=yes) %end;;
        set &m_caslib..top_5_gb_models;
        session_id = cfg_id;
        if session_id = 1 then output &m_caslib..best_cfg_session_1;
        %do group_act_process=2 %to &num_sessions.;
            else if session_id = &group_act_process. then output &m_caslib..best_cfg_session_&group_act_process.;
        %end;
    run;
    %do group_act_process=1 %to &num_sessions.;
        signon task_&group_act_process. sascmd="!sascmd -nosyntaxcheck -noterminal";
        %syslput _global_/like='*' remote=task_&group_act_process.;
        %syslput _local_/like='*' remote=task_&group_act_process.;
    %end;
    %do group_act_process=1 %to &num_sessions;

        rsubmit task_&group_act_process. wait=no;
        options MSGLEVEL=I NOFULLSTIMER formchar = '|----|+|---';
		options OBS=MAX NOSYNTAXCHECK REPLACE NOQUOTELENMAX;
            %include "&_root_path/Sources/Macros/LogsConfiguration.sas";
            %if &seg = 0 %then %do;
                %config_log(m_gb_model_&group_act_process., &&_logs_tcl_&tr);
            %end;
            %else %do;
                %config_log(m_gb_model_&group_act_process., &&_logs_tcl_&tr._seg_&seg);
            %end;
            cas casr&group_act_process. sessopts=(caslib="&m_caslib");
            libname &m_caslib. cas caslib=&m_caslib.;
            options casdatalimit=ALL;
            %include "&_root_path/Sources/Macros/_gini.sas";
            %include "&_root_path/Sources/Macros/_calculate_gini.sas";
            %include "&_root_path/Sources/Macros/_get_gini_mensual.sas";
            %include "&_root_path/Sources/Modulos/m_gradient_boosting/__gb_train.sas";
            %include "&_root_path/Sources/Macros/_create_caslib.sas";
            %include "&_root_path/Sources/Macros/_drop_caslib.sas";

            %__gb_train(1);
            cas casr&group_act_process. terminate;
            %Config_Log_Restore;
        endrsubmit;
    %end;

    /* Esperar a que todas las tareas terminen */
    waitfor _ALL_ %do group_act_process = 1 %to &num_sessions.; 
        task_&group_act_process. %end;;
    /* Cerrar sesiones paralelas */
    %do group_act_process = 1 %to &num_sessions.;
        signoff task_&group_act_process.;
    %end;

    %_drop_caslib_table(
        m_cas_session=&m_session.,
        m_caslib=&m_caslib.,
        m_castable_name=best_cfg_session,
        is_loop=1
    );

    %include "&_root_path/Sources/Macros/_merge_ginis_mensual.sas";
    %_merge_ginis_mensual(byvarl=&m_time., top_models=5, app_bhv_flg=&m_model_type., mlmodel=Gradient Boosting);
    proc sort data=&m_caslib..top_5_gb_models out=top_5_gb_models_sort; by descending gini_penalizado;run;
    title "Top 5 modelos Gradient Boosting por Gini Penalizado";
    proc print data=top_5_gb_models_sort noobs;run;
    title;

    %include "&_root_path/Sources/Modulos/m_champion_challenge/_save_metadata_model.sas";
    %_save_metadata_model(
        tro_var=&m_troncal.,
        seg_name=&gb_var_seg.,
        metadata_path=&gb_data_path.,
        modelabrv=gb,
        modelo_name= Gradient Boosting,
        segment_var =&m_var_seg.
    );

    %_drop_caslib_table(
        m_cas_session=&m_session.,
        m_caslib=&m_caslib.,
        m_castable_name=top_5_gb_models,
        is_loop=0
    );
    %_drop_caslib_table(
        m_cas_session=&m_session.,
        m_caslib=&m_caslib.,
        m_castable_name=report,
        is_loop=1,
        start=0
    );
    proc datasets lib=work nodetails nolist;
        delete _gini_mensual_bmk_train best_cfg best_gb_results bestconfiguration evaluationhistory hist_sorted top_5_gb_models: gini_: t_: report_:;
    run;

    %_drop_caslib(
        caslib_name =&m_caslib.,
        del_prom_tables = Y,
        cas_sess_name =&m_session.,
        terminate_session = Y
    );
%mend;
%macro __gb_set_ranges(caslib_name=, tabla=, num=, cat=, ntrees_cap=800);
    /*--------------------------------------------------------------------
     Gradient Boosting hyperparameter ranges
     Inputs:
        tabla=      libref.table con partición train (para contar obs)
        num=        lista de variables numéricas separadas por espacios
        cat=        lista de variables categóricas separadas por espacios
        ntrees_cap= máximo global permitido para NTREES
     Output (global macrovars):
        ntrees_lb/init/ub, lr_lb/init/ub, maxdepth_lb/init/ub, ...
        ntrees_bounds, lr_bounds, ... (cadenas "LB= INIT= UB=")
    --------------------------------------------------------------------*/

    /*---------------------------*/
    /* 1. nº de observaciones    */
    /*---------------------------*/
    data _null_;
        if 0 then set &caslib_name..&tabla. (where=(_partind_=1)) nobs=observations;
        call symputx('train_obs', observations);
        stop;
    run;

    %if %sysevalf(&train_obs <= 0) %then %do;
        %put ERROR: gb_set_ranges: tabla=&caslib_name..&tabla no tiene observaciones > 0;
        %return;
    %end;

    /*---------------------------*/
    /* 2. nº de variables input  */
    /*---------------------------*/
    %local total_inputs p_inputs;
    %let total_inputs = &num. &cat.;
    %let p_inputs = %sysfunc(countw(&total_inputs., %str( )));
    %put NOTE: Número de observaciones: &train_obs.;
    %put NOTE: Número de variables: &p_inputs.;

    /*---------------------------*/
    /* 3. clasif tamaño (S)      */
    /*---------------------------*/
    %local S;
    %if &train_obs <= 30000    %then %let S = 1;
    %else %if &train_obs <= 150000 %then %let S = 2;
    %else %if &train_obs <= 500000 %then %let S = 3;
    %else %let S = 4;

    /*---------------------------*/
    /* 4. clasif variables (P)   */
    /*---------------------------*/
    %local P;
    %if &p_inputs <= 20   %then %let P = 1;
    %else %if &p_inputs <= 60 %then %let P = 2;
    %else %let P = 3;

    /*==============================================================*/
    /* 5. tablas base por S (LB/INIT/UB)                            */
    /*==============================================================*/
    %macro _base(param, s1, s2, s3, s4);
        %global &param._LB &param._INIT &param._UB;
        %if &S = 1 %then %do;
            %let &param._LB   = %scan(&s1,1,/);
            %let &param._INIT = %scan(&s1,2,/);
            %let &param._UB   = %scan(&s1,3,/);
        %end;
        %else %if &S = 2 %then %do;
            %let &param._LB   = %scan(&s2,1,/);
            %let &param._INIT = %scan(&s2,2,/);
            %let &param._UB   = %scan(&s2,3,/);
        %end;
        %else %if &S = 3 %then %do;
            %let &param._LB   = %scan(&s3,1,/);
            %let &param._INIT = %scan(&s3,2,/);
            %let &param._UB   = %scan(&s3,3,/);
        %end;
        %else %do;
            %let &param._LB   = %scan(&s4,1,/);
            %let &param._INIT = %scan(&s4,2,/);
            %let &param._UB   = %scan(&s4,3,/);
        %end;
    %mend _base;

    %_base(NTREES , 100/250/400 , 200/400/600 , 300/500/800 , 300/600/1000);
    %_base(LR     , 0.05/0.07/0.10 , 0.04/0.06/0.08 , 0.03/0.05/0.07 , 0.02/0.03/0.06);
    %_base(MAXDEP , 2/3/4 , 2/3/5 , 2/3/5 , 2/3/5);
    %_base(LEAF   , 25/60/100 , 50/120/200 , 100/150/200 , 150/200/200);
    %_base(SRATE  , 0.70/0.85/1.00 , 0.65/0.80/0.90 , 0.60/0.75/0.85 , 0.55/0.70/0.80);
    %_base(VARSTR , 3/6/9 , 5/10/15 , 5/10/20 , 10/15/25);
    %_base(LASSO  , 0/0.04/0.10 , 0/0.05/0.12 , 0/0.06/0.15 , 0/0.07/0.15);
    %_base(RIDGE  , 0/0.6/2 , 0/1/3 , 0/1.5/4 , 0/2/5);
    %_base(NBIN   , 32/64/128 , 32/128/256 , 64/128/256 , 64/128/256);

    /*==============================================================*/
    /* 6. Ajustes por P                                             */
    /*==============================================================*/

    /*--- P=1 (≤20 vars): refinar VARSTR para p muy pequeño -------*/
    %if &P = 1 %then %do;
        /* Para p≤10: usar  max(1,2) / 3 / p  (p.ej. 2/3/6) */
        %if &p_inputs <= 10 %then %do;
            %let VARSTR_LB   = %sysfunc(min(&p_inputs, %sysfunc(max(1,2))));
            %let VARSTR_INIT = %sysfunc(min(&p_inputs, 3));
            %let VARSTR_UB   = &p_inputs;
        %end;
        /* Para 11-20: mantener base (3/6/9) pero no >p */
    %end;

    /*--- P=2 (21-60 vars) ----------------------------------------*/
    %else %if &P = 2 %then %do;
        /* MAXDEPTH UB +1 (cap 6) */
        %let MAXDEP_UB = %sysfunc(min(6, %sysevalf(&MAXDEP_UB + 1)));
        /* VARSTR LB -1 (cap ≥1) */
        %let VARSTR_LB = %sysfunc(max(1, %sysevalf(&VARSTR_LB - 1)));
    %end;

    /*--- P=3 (>60 vars) ------------------------------------------*/
    %else %do;
        /* más columnas: ampliar búsqueda */
        %let NTREES_LB   = %eval(&NTREES_LB   + 100);
        %let NTREES_INIT = %eval(&NTREES_INIT + 100);
        %let NTREES_UB   = %eval(&NTREES_UB   + 200);

        %let LR_LB   = %sysfunc(max(0.01,%sysevalf(&LR_LB   - 0.01)));
        %let LR_INIT = %sysfunc(max(0.01,%sysevalf(&LR_INIT - 0.01)));
        %let LR_UB   = %sysfunc(max(0.01,%sysevalf(&LR_UB   - 0.01)));

        %let MAXDEP_UB = %sysfunc(min(6,%sysevalf(&MAXDEP_UB + 1)));

        %let LEAF_LB   = %eval(&LEAF_LB   + 25);
        %let LEAF_INIT = %eval(&LEAF_INIT + 50);

        %let SRATE_LB   = %sysevalf(&SRATE_LB   - 0.05);
        %let SRATE_INIT = %sysevalf(&SRATE_INIT - 0.05);
        %let SRATE_UB   = %sysevalf(&SRATE_UB   - 0.05);

        %let VARSTR_LB   = %eval(&VARSTR_LB   + 5);
        %let VARSTR_INIT = %eval(&VARSTR_INIT + 5);
        %let VARSTR_UB   = %eval(&VARSTR_UB   + 5);

        %let LASSO_INIT = %sysfunc(min(0.20,%sysevalf(&LASSO_INIT + 0.02)));
        %let LASSO_UB   = %sysfunc(min(0.20,%sysevalf(&LASSO_UB   + 0.02)));

        %let RIDGE_INIT = %sysevalf(&RIDGE_INIT + 0.5);
        %let RIDGE_UB   = %sysevalf(&RIDGE_UB   + 0.5);
    %end;

    /*==============================================================*/
    /* 7. Caps dependientes de datos y orden LB<=INIT<=UB           */
    /*==============================================================*/

    /* NTREES cap global */
    %if &NTREES_UB > &ntrees_cap %then %let NTREES_UB = &ntrees_cap;
    %if &NTREES_INIT > &NTREES_UB %then %let NTREES_INIT = &NTREES_UB;
    %if &NTREES_LB   > &NTREES_INIT %then %let NTREES_LB = &NTREES_INIT;

    /* VARSTR ≤ p_inputs, ≥1 */
    %if &VARSTR_UB > &p_inputs %then %let VARSTR_UB = &p_inputs;
    %if &VARSTR_INIT > &p_inputs %then %let VARSTR_INIT = &p_inputs;
    %if &VARSTR_LB > &p_inputs %then %let VARSTR_LB = &p_inputs;
    %if &VARSTR_LB < 1 %then %let VARSTR_LB = 1;
    %if &VARSTR_INIT < &VARSTR_LB %then %let VARSTR_INIT = &VARSTR_LB;
    %if &VARSTR_UB   < &VARSTR_INIT %then %let VARSTR_UB = &VARSTR_INIT;

    /* LEAF dentro [1,train_obs] */
    %if &LEAF_UB   > &train_obs %then %let LEAF_UB   = &train_obs;
    %if &LEAF_INIT > &LEAF_UB   %then %let LEAF_INIT = &LEAF_UB;
    %if &LEAF_LB   > &LEAF_INIT %then %let LEAF_LB   = &LEAF_INIT;
    %if &LEAF_LB   < 1          %then %let LEAF_LB   = 1;

    /* SRATE [0.1,1] */
    %let SRATE_LB   = %sysfunc(max(0.1,%sysfunc(min(1,&SRATE_LB))));
    %let SRATE_INIT = %sysfunc(max(0.1,%sysfunc(min(1,&SRATE_INIT))));
    %let SRATE_UB   = %sysfunc(max(0.1,%sysfunc(min(1,&SRATE_UB))));
    %if &SRATE_LB > &SRATE_INIT %then %let SRATE_LB = &SRATE_INIT;
    %if &SRATE_INIT > &SRATE_UB %then %let SRATE_INIT = &SRATE_UB;

    /* LR [0.01,1] */
    %let LR_LB   = %sysfunc(max(0.01,%sysfunc(min(1,&LR_LB))));
    %let LR_INIT = %sysfunc(max(0.01,%sysfunc(min(1,&LR_INIT))));
    %let LR_UB   = %sysfunc(max(0.01,%sysfunc(min(1,&LR_UB))));
    %if &LR_LB > &LR_INIT %then %let LR_LB = &LR_INIT;
    %if &LR_INIT > &LR_UB %then %let LR_INIT = &LR_UB;

    /* MAXDEP [1,6] */
    %let MAXDEP_LB   = %sysfunc(max(1,%sysfunc(min(6,&MAXDEP_LB))));
    %let MAXDEP_INIT = %sysfunc(max(1,%sysfunc(min(6,&MAXDEP_INIT))));
    %let MAXDEP_UB   = %sysfunc(max(1,%sysfunc(min(6,&MAXDEP_UB))));
    %if &MAXDEP_LB > &MAXDEP_INIT %then %let MAXDEP_LB = &MAXDEP_INIT;
    %if &MAXDEP_INIT > &MAXDEP_UB %then %let MAXDEP_INIT = &MAXDEP_UB;

    /* LASSO [0,0.20] */
    %let LASSO_LB   = %sysfunc(max(0,%sysfunc(min(0.20,&LASSO_LB))));
    %let LASSO_INIT = %sysfunc(max(0,%sysfunc(min(0.20,&LASSO_INIT))));
    %let LASSO_UB   = %sysfunc(max(0,%sysfunc(min(0.20,&LASSO_UB))));
    %if &LASSO_LB > &LASSO_INIT %then %let LASSO_LB = &LASSO_INIT;
    %if &LASSO_INIT > &LASSO_UB %then %let LASSO_INIT = &LASSO_UB;

    /* RIDGE [0,5] */
    %let RIDGE_LB   = %sysfunc(max(0,%sysfunc(min(5,&RIDGE_LB))));
    %let RIDGE_INIT = %sysfunc(max(0,%sysfunc(min(5,&RIDGE_INIT))));
    %let RIDGE_UB   = %sysfunc(max(0,%sysfunc(min(5,&RIDGE_UB))));
    %if &RIDGE_LB > &RIDGE_INIT %then %let RIDGE_LB = &RIDGE_INIT;
    %if &RIDGE_INIT > &RIDGE_UB %then %let RIDGE_INIT = &RIDGE_UB;

    /* NBIN [10,256] */
    %let NBIN_LB   = %sysfunc(max(10,%sysfunc(min(256,&NBIN_LB))));
    %let NBIN_INIT = %sysfunc(max(10,%sysfunc(min(256,&NBIN_INIT))));
    %let NBIN_UB   = %sysfunc(max(10,%sysfunc(min(256,&NBIN_UB))));
    %if &NBIN_LB > &NBIN_INIT %then %let NBIN_LB = &NBIN_INIT;
    %if &NBIN_INIT > &NBIN_UB %then %let NBIN_INIT = &NBIN_UB;

    /*============================================*/
    /* 9. Cadena bounds                           */
    /*============================================*/
    %global ntrees_bounds lr_bounds maxdepth_bounds minleaf_bounds
            ssrate_bounds vars_to_try_bounds lasso_bounds ridge_bounds
            bins_bounds maxbranch_bounds;

    %let ntrees_bounds = LB=&NTREES_LB. UB=&NTREES_UB. INIT=&NTREES_INIT.;
    %let maxdepth_bounds = LB=&MAXDEP_LB. UB=&MAXDEP_UB. INIT=&MAXDEP_INIT.;
    %let minleaf_bounds = LB=&LEAF_LB. UB=&LEAF_UB. INIT=&LEAF_INIT.;
    %let vars_to_try_bounds = LB=&VARSTR_LB. UB=&VARSTR_UB. INIT=&VARSTR_INIT.;
    %let bins_bounds = LB=&NBIN_LB. UB=&NBIN_UB. INIT=&NBIN_INIT.;
    %let lasso_bounds = LB=&LASSO_LB. UB=&LASSO_UB. INIT=&LASSO_INIT.;
    %let ridge_bounds = LB=&RIDGE_LB. UB=&RIDGE_UB. INIT=&RIDGE_INIT.;
    %let lr_bounds = LB=&LR_LB. UB=&LR_UB. INIT=&LR_INIT.;
    %let ssrate_bounds = LB=&SRATE_LB. UB=&SRATE_UB. INIT=&SRATE_INIT.;
    %let maxbranch_bounds = 2;


    %put tabla=&caslib_name..&tabla nobs=&train_obs p=&p_inputs (S=&S P=&P);
    %put NTREES     = &ntrees_bounds;
    %put LRGRATE    = &lr_bounds;
    %put MDEPTH     = &maxdepth_bounds;
    %put MINLEAF    = &minleaf_bounds;
    %put SAMPLING   = &ssrate_bounds;
    %put VARSTTRY   = &vars_to_try_bounds;
    %put LASSO      = &lasso_bounds;
    %put RIDGE      = &ridge_bounds;
    %put NBIN       = &bins_bounds;
    %put BRANCH     = &maxbranch_bounds;


%mend;
%macro __gb_tune(
    caslib_name=,
    tabla=,
    num_input=,
    cat_input=,
    target_input=,
    gb_stagnation=4
);
    ods exclude all;
    proc gradboost data=&caslib_name..&tabla.
        maxbranch=&maxbranch_bounds.
        seed=12345
        earlystop(metric=LOGLOSS stagnation=&gb_stagnation.);
        partition rolevar=_PartInd_(train='1' validate='0' test='2');
        input &num_input. / level=interval;
        %if %length(&cat_input.)>0 %then %do; input &cat_input. / level=nominal; %end;
        target &target_input. / level=nominal;
        autotune
            historytable=&caslib_name..evaluationhistory
            evalhistory=all
            targetevent="1"
            objective=gini
            searchmethod=BAYESIAN
            NPARALLEL=5
            useparameters=custom	
            tuningparameters=(
                lasso(&lasso_bounds.)
                learningrate(&lr_bounds.)
                maxdepth(&maxdepth_bounds.)
                minleafsize(&minleaf_bounds.)
                ntrees(&ntrees_bounds.)
                numbin(&bins_bounds.)
                ridge(&ridge_bounds.)
                samplingrate(&ssrate_bounds.)
                vars_to_try(&vars_to_try_bounds.)
            );
        ods output BestConfiguration=work.bestconfiguration;
        ods output EvaluationHistory=work.evaluationhistory;
    run;
    ods exclude none;
    data evaluationhistory;
		set evaluationhistory;
		maxlevel = maxlevel - 1;
	run;

%mend __gb_tune;
%macro __gb_train(is_top_flg);
    /* Identificadores para esta sesión */
    %let ses_p = &group_act_process.;
    %let caslib = casuser;
    %let is_top = &is_top_flg.;
    %let ses_tro = &tr_sess.;
    %let ses_seg = &seg_sess.;
    %let bmk_path = &global_bmk_path.;
    %let process_var_seg = &gb_var_seg.;
    /* Crear tabla de resultados para esta sesión */
    %put la flag de si  es top es: &is_top.;

    proc sql;
        create table GB_RESULTS_SESSION_&ses_p(
            cfg_id num,
            %if &m_gb_stagnation. eq 0 %then %do; NTREE num, %end;
            M num,
            LEARNINGRATE num format=best32.,
            SUBSAMPLERATE num format=best32.,
            LASSO num format=best32.,
            RIDGE num format=best32.,
            NBINS num,
            MAXLEVEL num,
            LEAFSIZE num,
            gini_train num,
            gini_oot num,
            gini_penalizado num
        );
    quit;
    /* Procesar cada modelo asignado a esta sesión */
    %global n_models;
    proc sql noprint;
        select count(*) into :n_models from &caslib..best_cfg_session_&ses_p.;
    quit;
    %put [SESSION &ses_p] Procesando &n_models modelos;
    
    /* Procesar cada modelo asignado a esta sesion */
    %do m = 1 %to &n_models;
        %put [SESSION &ses_p] Procesando modelo &m de &n_models;
        
        /* Extraer parametros del modelo */
        data _null_;
            set &caslib..best_cfg_session_&ses_p(firstobs=&m obs=&m);
            call symputx('model_id', cfg_id);
            %if &m_gb_stagnation. eq 0 %then %do; call symputx('best_ntree', NTREE); %end;
            call symputx('best_vars', M);
            call symputx('best_learningrate', LEARNINGRATE);
            call symputx('best_subsamplerate', SUBSAMPLERATE);
            call symputx('best_lasso', LASSO);
            call symputx('best_ridge', RIDGE);
            call symputx('best_nbins', NBINS);
            call symputx('best_maxdepth', MAXLEVEL);
            call symputx('best_leafsize', LEAFSIZE);
        run;
        %put [SESSION &ses_p] Se procesan los parametros para el modelo &model_id;
        %put [SESSION &ses_p] &model_id - &best_vars - &best_learningrate - &best_subsamplerate - &best_lasso - &best_ridge - &best_nbins - &best_maxdepth - &best_leafsize;
        /* Entrenar el modelo Gradient Boosting */
        ods exclude all;
        proc gradboost data=&caslib..train_part
            maxbranch=2
            seed=12345
            l1=&best_lasso.
            l2=&best_ridge.
            learningrate=&best_learningrate.
            maxdepth=&best_maxdepth.
            minleafsize=&best_leafsize.
            %if &m_gb_stagnation. eq 0 %then %do; ntrees=&best_ntree %end;
            numbin=&best_nbins.
            samplingrate=&best_subsamplerate.
            vars_to_try=&best_vars.
            earlystop(metric=LOGLOSS stagnation=&m_gb_stagnation.);
            partition rolevar=_PartInd_(train='1' validate='0' test='2');
            input &m_num_inputs. / level=interval;
            %if %length(&m_cat_inputs.)>0 %then %do; input &m_cat_inputs. / level=nominal; %end;
            target &m_target. / level=nominal;
            savestate rstore=&caslib..gb_sess&ses_p._&model_id;
        run;
        /* Luego usa PROC ASTORE */
        proc astore;
            score data=&caslib..&m_train. out=&caslib..train_scored_gb_&model_id
            rstore=&caslib..gb_sess&ses_p._&model_id copyvars=(&m_target. &m_time.);
        run;
        
        proc astore;
            score data=&caslib..&m_oot. out=&caslib..oot_scored_gb_&model_id
            rstore=&caslib..gb_sess&ses_p._&model_id copyvars=(&m_target. &m_time.);
        run;
        ods exclude none;

        %if &is_top. eq 1 %then %do;
            %put [SESSION &ses_p] Se promovió el modelo &model_id a &caslib;
            %if &model_id. eq 1 %then %do;
                %_create_caslib(
                    cas_path =&bmk_path.,
                    caslib_name =bmk,
                    lib_caslib =bmk,
                    global = Y,
                    cas_sess_name =casr&ses_p.,
                    keep_sess = Y
                );
                %put DEV: se creo el caslib;
                %local process_var_seg_final;
                %if %sysevalf(%superq(process_var_seg)=,boolean) %then %let process_var_seg_final=UNIVERSE;
                %else %let process_var_seg_final=&process_var_seg;
                
                proc casutil;
                    save casdata="gb_sess&ses_p._&model_id" incaslib="&caslib"
                            casout="&process_var_seg_final._gb" 
                            outcaslib="bmk" replace;
                quit;
                %PUT DEV: SE GUARDO EL MODELO;
                %_drop_caslib(
                    caslib_name =bmk,
                    del_prom_tables = N,
                    cas_sess_name =casr&ses_p.,
                    terminate_session = N,
                    drop_caslib=Y
                );                
            %end;

        %end;

        %put [SESSION &ses_p] Se scoreo train y oot usando el modelo &model_id;

        /* Cálculo de Gini */
        %let predvar = P_&m_target.1;

        %_gini(&caslib., train_scored_gb_&model_id., &m_target., &predvar., g_tr);
        %_gini(&caslib., oot_scored_gb_&model_id., &m_target., &predvar., g_oot);

        %let lambda = 0.5;
        %let g_penalized = %sysevalf(&g_oot. - %sysevalf(&lambda. * %sysevalf(&g_tr. - &g_oot.)));

        %put [SESSION &ses_p] Modelo &model_id - GINI TRAIN: &g_tr - GINI OOT: &g_oot;
        
        /* Guardar resultados */
        proc sql;
            insert into GB_RESULTS_SESSION_&ses_p values(
                &model_id,
                %if &m_gb_stagnation. eq 0 %then %do; &best_ntree, %end;
                &best_vars,
                &best_learningrate,
                &best_subsamplerate,
                &best_lasso,
                &best_ridge,
                &best_nbins,
                &best_maxdepth,
                &best_leafsize,
                &g_tr,
                &g_oot,
                &g_penalized
            );
        quit;      
        %if &is_top. eq 1 %then %do;
            data &caslib..full_scored_gb; 
                set &caslib..train_scored_gb_&model_id &caslib..oot_scored_gb_&model_id; 
            run;
            %_get_gini_mensual(&caslib., full_scored_gb, &predvar., &m_target., &model_id., byvarl = &m_time.);
        %end;
    %end;
    data casuser.GB_RESULTS_SESSION_&ses_p(copies=0 promote=yes);
        set GB_RESULTS_SESSION_&ses_p;
    run;

    %put [SESSION &ses_p] Todos los modelos han sido procesados;

%mend __gb_train;

%include "&_root_path/Sources/Modulos/m_gradient_boosting/__gb_report.sas";

%macro __gb_verify(v_train, v_oot, v_troncal, v_segmento);
    /* Verificación de existencia de datasets */
    %let proceed = 1;
    %let train_exists = %sysfunc(exist(&v_train));
    %let oot_exists = %sysfunc(exist(&v_oot));
    
    /* Verificar existencia de datasets */
    %if &train_exists = 0 %then %do;
        %put WARNING DEVELOPER: El dataset de entrenamiento &v_train no existe;
        %let proceed = 0;
    %end;
    %else %do;
        /* Verificar que tenga registros */
        %let train_nobs = %sysfunc(attrn(%sysfunc(open(&v_train)), NOBS));
        %if &train_nobs = 0 %then %do;
            %put WARNING DEVELOPER: El dataset de entrenamiento &v_train existe pero no contiene registros;
            %let proceed = 0;
        %end;
        %else %do;
            %put NOTE: Dataset &v_train validado correctamente con &train_nobs registros;
        %end;
    %end;
    
    %if &oot_exists = 0 %then %do;
        %put WARNING DEVELOPER: El dataset OOT &v_oot no existe;
        %let proceed = 0;
    %end;
    %else %do;
        /* Verificar que tenga registros */
        %let oot_nobs = %sysfunc(attrn(%sysfunc(open(&v_oot)), NOBS));
        %if &oot_nobs = 0 %then %do;
            %put WARNING DEVELOPER: El dataset OOT &v_oot existe pero no contiene registros;
            %let proceed = 0;
        %end;
        %else %do;
            %put NOTE: Dataset &v_oot validado correctamente con &oot_nobs registros;
        %end;
    %end;
    
    /* Verificación de variables críticas */
    %if %sysevalf(&_target=,boolean) %then %do;
        %put WARNING DEVELOPER: La variable target (&_target) está vacía;
        %let proceed = 0;
    %end;
    
    %if %sysevalf(&_var_time=,boolean) %then %do;
        %put WARNING DEVELOPER: La variable tiempo (&_var_time) está vacía;
        %let proceed = 0;
    %end;

    %if %sysevalf(&var_pd=,boolean) %then %do;
        %put WARNING DEVELOPER: La variable PD (&var_pd) está vacía;
        %let proceed = 0;
    %end;
    
    /* Verificar que al menos un tipo de variables (categóricas o numéricas) no esté vacío */
    %if %sysevalf(&vars_num=,boolean) and %sysevalf(&vars_cat=,boolean) %then %do;
        %put WARNING DEVELOPER: Debe existir al menos una lista de variables categóricas o numéricas;
        %let proceed = 0;
    %end;
    
    /* Mostrar información del troncal y segmento */
    %put NOTE: Ejecutando validaciones para Troncal: &v_troncal, Segmento: &v_segmento;
    
    /* Si todas las validaciones pasan, ejecutar el reporte */
    %if &proceed = 1 %then %do;
        %put NOTE: Todas las validaciones pasaron correctamente. Ejecutando módulo de reporte...;
        
        %__gb_report(
            r_train=&v_train.,
            r_oot=&v_oot.,
            r_target=&_target.,
            r_time=&_var_time.,
            r_xb_pd=&var_pd.,
            r_num=&vars_num.,
            r_cat=&vars_cat.,
            r_troncal=&v_troncal.,
            r_segmento=&v_segmento.,
            r_var_seg=&var_segmentadora.,
            r_model_type=&_tipo_modelo.
        );
    %end;
    %else %do;
        %put WARNING DEVELOPER: No se pudo ejecutar el módulo de reporte debido a WARNING DEVELOPERes en la validación;
    %end;
%mend;
/*---------------------------------------------------------------------------
  Version: 2.0	  
  Desarrollador: Joseph Chombo					
  Fecha Release: 06/10/2025
-----------------------------------------------------------------------------*/

%include "&_root_path/Sources/Modulos/m_gradient_boosting/gb_challenge_macro.sas";

%macro __gb_report(r_train=, r_oot=, r_target=, r_time=, r_xb_pd=, r_num=, r_cat=, r_troncal=, r_segmento=, r_var_seg=, r_model_type=);

    ods graphics on / outputfmt=svg;
    /* Iniciar nuevo archivo Excel con hoja para TRAIN */
    ods html5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Gb_challenge_1.html";	
    ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Gb_challenge.xlsx"
                options(sheet_name="Benchmark TRAIN OOT" 
                    sheet_interval="none" 
                    embedded_titles="yes");
       
        %gb_challenge_macro(
            m_train=&r_train.,
            m_oot=&r_oot.,
            m_target=&r_target.,
            m_time=&r_time.,
            m_xb_pd=&r_xb_pd.,
            m_num_inputs=&r_num.,
            m_cat_inputs=&r_cat.,
            m_troncal=&r_troncal.,
            m_segmento=&r_segmento.,
            m_var_seg=&r_var_seg.,
            m_model_type=&r_model_type.
        );
    ods excel close;
    ods html5 close;
%mend;