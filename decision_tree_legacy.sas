%macro __dt_set_ranges(
    caslib_name=,
    tabla=,
    num=,
    cat=,
    target=,                /* para estimar desbalance y ajustar MINLEAFSIZE */
    event=1,
    maxdepth_cap=12,        /* cota de seguridad para árboles individuales interpretables */
    numbin_cap=100          /* cota superior práctica para binning */
);
/* ----------------------------- */
/* 1) Nobs, NumVars, CatVars     */
/* ----------------------------- */
data _null_;
    if 0 then set &caslib_name..&tabla. (where=(_partind_=1)) nobs=observations;
    call symputx('Nobs', observations);
    stop;
run;

%if %sysevalf(&Nobs <= 0) %then %do;
    %put ERROR: __set_dt_ranges: tabla=&caslib_name..&tabla no tiene observaciones > 0;
    %return;
%end;

%let NumVars = %sysfunc(countw(&num, %str( )));
%put NOTE: Número de variables: &NumVars.;
%let CatVars = %sysfunc(countw(&cat, %str( )));
%put NOTE: Número de variables categoricas: &CatVars.;

/* ----------------------------- */
/* 2) Señales de contexto        */
/* ----------------------------- */
%global _S _P _C _I _event_rate;
%if &Nobs <= 20000 %then %let _S=1;         /* pequeño */
%else %if &Nobs <= 100000 %then %let _S=2;  /* mediano */
%else %if &Nobs <= 300000 %then %let _S=3;  /* grande */
%else %let _S=4;                            /* muy grande (hasta 500k) */

%if &NumVars <= 25 %then %let _P=1;         /* baja dimensión */
%else %if &NumVars <= 60 %then %let _P=2;   /* media */
%else %let _P=3;                             /* alta */

%global _cat_share;
%if &NumVars>0 %then %let _cat_share=%sysevalf(&CatVars/&NumVars);
%else %let _cat_share=0;
%if &_cat_share >= 0.50 %then %let _C=1;    /* muchas categóricas */
%else %if &_cat_share >= 0.20 %then %let _C=2;
%else %let _C=3;                             /* mayoría numéricas */

/* tasa de evento (si se provee target) */
%let _event_rate=.;
%if %length(&target) %then %do;
    proc sql noprint;
      select mean((&target=&event)*1) into :_event_rate trimmed
      from &caslib_name..&tabla
      where _partind_=1;
    quit;
%end;
/* desbalance: 1=extremo, 2=moderado, 3=balanceado */
%if %sysevalf(&_event_rate=.,boolean) %then %let _I=2;
%else %if &_event_rate < 0.03 or &_event_rate > 0.97 %then %let _I=1;
%else %if &_event_rate < 0.10 or &_event_rate > 0.90 %then %let _I=2;
%else %let _I=3;

/* ----------------------------- */
/* 3) CRITERION (lista discreta) */
/* ----------------------------- */
/* En PROC TREESPLIT el criterio se controla con GROW. Aquí exponemos un "CRITERION" lógico*/
%global CRITERION_LIST;
%let CRITERION_LIST = VALUES=GINI ENTROPY IGR INIT=GINI;

/* ----------------------------- */
/* 4) MAXDEPTH bounds            */
/* ----------------------------- */
%global MAXDEPTH_LB MAXDEPTH_INIT MAXDEPTH_UB;
%if &_S=1 %then %do;  /* <=20k */
  %let MAXDEPTH_LB=3;  %let MAXDEPTH_INIT=5;  %let MAXDEPTH_UB=8;
%end;
%else %if &_S=2 %then %do; /* 20k-100k */
  %let MAXDEPTH_LB=4;  %let MAXDEPTH_INIT=6;  %let MAXDEPTH_UB=9;
%end;
%else %do; /* >=100k */
  %let MAXDEPTH_LB=4;  %let MAXDEPTH_INIT=6;  %let MAXDEPTH_UB=10;
%end;
/* si muchas categóricas, contenga un poco la profundidad */
%if &_C=1 %then %let MAXDEPTH_UB=%sysfunc(min(&MAXDEPTH_UB,9));
/* cota global */
%let MAXDEPTH_LB  = %sysfunc(max(2,%sysfunc(min(&maxdepth_cap,&MAXDEPTH_LB))));
%let MAXDEPTH_INIT= %sysfunc(max(2,%sysfunc(min(&maxdepth_cap,&MAXDEPTH_INIT))));
%let MAXDEPTH_UB  = %sysfunc(max(2,%sysfunc(min(&maxdepth_cap,&MAXDEPTH_UB))));
%if &MAXDEPTH_LB>&MAXDEPTH_INIT %then %let MAXDEPTH_LB=&MAXDEPTH_INIT;
%if &MAXDEPTH_INIT>&MAXDEPTH_UB %then %let MAXDEPTH_INIT=&MAXDEPTH_UB;

/* ----------------------------- */
/* 5) MINLEAFSIZE bounds         */
/* ----------------------------- */
%global MINLEAF_LB MINLEAF_INIT MINLEAF_UB;
/* base por tamaño de muestra */
%if &_S=1 %then %do;  %let MINLEAF_LB=5;   %let MINLEAF_INIT=20;  %let MINLEAF_UB=%sysfunc(max(50,%sysfunc(int(%sysevalf(0.05*&Nobs))))); %end;
%else %if &_S=2 %then %do; %let MINLEAF_LB=10;  %let MINLEAF_INIT=30;  %let MINLEAF_UB=%sysfunc(max(80,%sysfunc(int(%sysevalf(0.03*&Nobs))))); %end;
%else %do;                    %let MINLEAF_LB=20;  %let MINLEAF_INIT=40;  %let MINLEAF_UB=%sysfunc(max(120,%sysfunc(int(%sysevalf(0.02*&Nobs))))); %end;
/* si desbalance extremo, permitir hojas más chicas */
%if &_I=1 %then %do;
  %let MINLEAF_LB=%sysfunc(max(5,%eval(&MINLEAF_LB/2)));
  %let MINLEAF_INIT=%sysfunc(max(10,%eval(&MINLEAF_INIT/2)));
%end;
/* orden y cotas */
%let MINLEAF_LB  = %sysfunc(max(1,%sysfunc(min(&Nobs,&MINLEAF_LB))));
%let MINLEAF_INIT= %sysfunc(max(1,%sysfunc(min(&Nobs,&MINLEAF_INIT))));
%let MINLEAF_UB  = %sysfunc(max(1,%sysfunc(min(&Nobs,&MINLEAF_UB))));
%if &MINLEAF_LB>&MINLEAF_INIT %then %let MINLEAF_LB=&MINLEAF_INIT;
%if &MINLEAF_INIT>&MINLEAF_UB %then %let MINLEAF_INIT=&MINLEAF_UB;

/* ----------------------------- */
/* 6) NUMBIN bounds              */
/* ----------------------------- */
%global NUMBIN_LB NUMBIN_INIT NUMBIN_UB;
/* base por tamaño y mezcla de tipos */
%if &_S=1 %then %do;  %let NUMBIN_LB=10; %let NUMBIN_INIT=20; %let NUMBIN_UB=40;  %end;
%else %if &_S=2 %then %do; %let NUMBIN_LB=10; %let NUMBIN_INIT=25; %let NUMBIN_UB=60;  %end;
%else %do;                    %let NUMBIN_LB=10; %let NUMBIN_INIT=30; %let NUMBIN_UB=80;  %end;
/* si muchas categóricas, bajar algo el UB (menos granularidad en continuas suele ser suficiente) */
%if &_C=1 %then %let NUMBIN_UB=%sysfunc(min(&NUMBIN_UB,60));
/* cota global */
%let NUMBIN_UB=%sysfunc(min(&numbin_cap,&NUMBIN_UB));
%if &NUMBIN_LB>&NUMBIN_INIT %then %let NUMBIN_LB=&NUMBIN_INIT;
%if &NUMBIN_INIT>&NUMBIN_UB %then %let NUMBIN_INIT=&NUMBIN_UB;

/* ----------------------------- */
/* 7) Macrovariables de salida   */
/* ----------------------------- */
%global maxdepth_bounds minleaf_bounds numbin_bounds
        assignmissing_opt binmethod_opt maxbranch_opt splitonce_opt grow_opt prune_opt
        info_dt;

%let maxdepth_bounds  = LB=&MAXDEPTH_LB INIT=&MAXDEPTH_INIT UB=&MAXDEPTH_UB;
%let minleaf_bounds   = LB=&MINLEAF_LB  INIT=&MINLEAF_INIT  UB=&MINLEAF_UB;
%let numbin_bounds    = LB=&NUMBIN_LB   INIT=&NUMBIN_INIT   UB=&NUMBIN_UB;

/* Recomendaciones SAS orientadas a maximizar Gini con generalización */
%let assignmissing_opt = USEINSEARCH;   /* tratar missing como nivel potencialmente informativo */
%let binmethod_opt     = QUANTILE;      /* binning por cuantiles en continuas */
%let maxbranch_opt     = 2;             /* divisiones binarias */
%let splitonce_opt     = FALSE;         /* permitir reusar variables en distintos nodos */
%let grow_opt          = GINI;          /* criterio de crecimiento por impureza Gini */
%let prune_opt         = COSTCOMPLEXITY;/* poda por complejidad (o C45 si hay validación explícita) */

%let info_dt = tabla=&caslib_name..&tabla Nobs=&Nobs NumVars=&NumVars CatVars=&CatVars
               event_rate=%sysfunc(round(&_event_rate,0.0001))
               S=&_S P=&_P C=&_C I=&_I
               MAXDEPTH=(&maxdepth_bounds)
               MINLEAFSIZE=(&minleaf_bounds) NUMBIN=(&numbin_bounds)
               OPTS=(ASSIGNMISSING=&assignmissing_opt BINMETHOD=&binmethod_opt
                     MAXBRANCH=&maxbranch_opt SPLITONCE=&splitonce_opt
                     GROW=&grow_opt PRUNE=&prune_opt);

%put NOTE: &info_dt;
%put NOTE: CRITERION LIST => &CRITERION_LIST;
%put NOTE: BOUNDS => MAXDEPTH(&maxdepth_bounds) MINLEAFSIZE(&minleaf_bounds) NUMBIN(&numbin_bounds);

%mend __dt_set_ranges;
%macro __dt_train(is_top_flg);
    /* Identificadores para esta sesión */
    %let ses_p = &group_act_process.;
    %let caslib = casuser;
    %let is_top = &is_top_flg.;
    %let ses_tro = &tr_sess.;
    %let ses_seg = &seg_sess.;
    %let bmk_path = &global_bmk_path.;
    %let process_var_seg = &dt_var_seg.;
    /* Crear tabla de resultados para esta sesión */
    %put la flag de si  es top es: &is_top.;

    proc sql;
        create table DT_RESULTS_SESSION_&ses_p(
            cfg_id num,
            MAXLEVEL num,
            NBINS num,
            LEAFSIZE num,
            MAXBRANCH num,
            CRIT char(12),
            MISSING char(12),
            BINMETHOD char,
            PRUNE char(14),
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
            call symputx('best_maxdepth', MAXLEVEL);
            call symputx('best_nbins', NBINS);
            call symputx('best_leafsize', LEAFSIZE);
            call symputx('best_maxbranch', MAXBRANCH);
            call symputx('best_criterion', CRIT);
            call symputx('best_missing', MISSING);
            call symputx('best_binmethod', BINMETHOD);
            call symputx('best_prune', PRUNE);
        run;
        %put [SESSION &ses_p] Se procesan los parametros para el modelo &model_id;
        %put [SESSION &ses_p] &model_id - &best_maxdepth - &best_nbins - &best_leafsize - &best_maxbranch - &best_missing - &best_binmethod - &best_prune;
        %put [SESSION &ses_p] Inicio del modelo &model_id %sysfunc(datetime(), datetime.);

        /* Entrenar el modelo dt */
        ods exclude all;
        proc treesplit data=&caslib..train_part
            assignmissing=&best_missing.
            binmethod=&best_binmethod.
            maxbranch=&best_maxbranch.
            maxdepth=&best_maxdepth.
            minleafsize=&best_leafsize.
            numbin=&best_nbins.
            seed=12345;
            grow &best_criterion.;
            prune &best_prune.;
            partition rolevar=_PartInd_(train='1' validate='0' test='2');
            input &m_num_inputs. / level=interval;
            %if %length(&m_cat_inputs.)>0 %then %do; input &m_cat_inputs. / level=nominal; %end;
            target &m_target. / level=nominal;
            savestate rstore=&caslib..dt_sess&ses_p._&model_id;            
        run;    
        /* Luego usa PROC ASTORE */
        proc astore;
            score data=&caslib..&m_train. out=&caslib..train_scored_dt_&model_id
            rstore=&caslib..dt_sess&ses_p._&model_id copyvars=(&m_target. &m_time.);
        run;
        
        proc astore;
            score data=&caslib..&m_oot. out=&caslib..oot_scored_dt_&model_id
            rstore=&caslib..dt_sess&ses_p._&model_id copyvars=(&m_target. &m_time.);
        run;
        ods exclude none;

        %if &is_top. eq 1 %then %do;
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
                    save casdata="dt_sess&ses_p._&model_id" incaslib="&caslib"
                            casout="&process_var_seg_final._dt" 
                            outcaslib="bmk" replace;
                quit;
                %PUT DEV: SE GUARDO EL MODELO;
                %_drop_caslib(
                    caslib_name =bmk,
                    del_prom_tables = N,
                    cas_sess_name =&casr&ses_p.,
                    terminate_session = N,
                    drop_caslib=Y
                );                   

            %end;

        %end;

        %put [SESSION &ses_p] Se scoreo train y oot usando el modelo &model_id;

        /* Cálculo de Gini */
        %let predvar = P_&m_target.1;

        %_gini(&caslib., train_scored_dt_&model_id., &m_target., &predvar., g_tr);
        %_gini(&caslib., oot_scored_dt_&model_id., &m_target., &predvar., g_oot);

        %let lambda = 0.5;
        %let g_penalized = %sysevalf(&g_oot. - %sysevalf(&lambda. * %sysevalf(&g_tr. - &g_oot.)));

        %put [SESSION &ses_p] Modelo &model_id - GINI TRAIN: &g_tr - GINI OOT: &g_oot;
        
        /* Guardar resultados */
        proc sql;
            insert into DT_RESULTS_SESSION_&ses_p values(
                &model_id,
                &best_maxdepth,
                &best_nbins,
                &best_leafsize,
                &best_maxbranch,
                "&best_criterion.",
                "&best_missing.",
                "&best_binmethod.",
                "&best_prune.",
                &g_tr,
                &g_oot,
                &g_penalized
            );
        quit;      
        %if &is_top. eq 1 %then %do;
            data &caslib..full_scored_dt; 
                set &caslib..train_scored_dt_&model_id &caslib..oot_scored_dt_&model_id; 
            run;
            %_get_gini_mensual(&caslib., full_scored_dt, &predvar., &m_target., &model_id., byvarl = &m_time.);
        %end;
    %end;
    data casuser.DT_RESULTS_SESSION_&ses_p(copies=0 promote=yes);
        set DT_RESULTS_SESSION_&ses_p;
    run;

    %put [SESSION &ses_p] Todos los modelos han sido procesados;
    %put [SESSION &ses_p] Fin de __dt_train %sysfunc(datetime(), datetime.);

%mend __dt_train;
%macro __dt_tune(
    caslib_name=,
    tabla=,
    num_input=,
    cat_input=,
    target_input=
);
    ods exclude all;
    proc treesplit data=&caslib_name..&tabla.
        assignmissing=&assignmissing_opt.
        binmethod=&binmethod_opt.
        maxbranch=&maxbranch_opt.
        seed=12345;
        prune &prune_opt.;
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
                criterion(&criterion_list.)
                maxdepth(&maxdepth_bounds.)
                minleafsize(&minleaf_bounds.)
                numbin(&numbin_bounds.)
            );
        ods output BestConfiguration=work.bestconfiguration;
        ods output EvaluationHistory=work.evaluationhistory;            
    run;
    ods exclude none;

%mend __dt_tune;
%macro dt_challenge_macro(
    m_train=,
    m_oot=,
    m_target=,
    m_time=,
    m_xb_pd=,
    m_num_inputs=,
    m_cat_inputs=,
    m_top_k=50,
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
        ml_algo   = Decision Tree
    );

    %include "&_root_path/Sources/Macros/_get_gini_mensual.sas";
    %_get_gini_mensual(&m_caslib., full_data, &m_xb_pd., &m_target., 0, byvarl = &m_time.);

    %include "&_root_path/Sources/Modulos/m_decision_tree/__dt_set_ranges.sas";
    %__dt_set_ranges(caslib_name=&m_caslib., tabla=full_data, num=&m_num_inputs., cat=&m_cat_inputs., target=&m_target.);
    
    %include "&_root_path/Sources/Modulos/m_decision_tree/__dt_tune.sas";
    %__dt_tune(caslib_name=&m_caslib., tabla=full_data, num_input=&m_num_inputs., cat_input=&m_cat_inputs., target_input=&m_target.);
    

    /* Selección de los mejores modelos */
    proc sort data=evaluationhistory out=hist_sorted; by descending GiniCoefficient; run;
    data best_cfg;
        set hist_sorted(obs=&m_top_k.);
        MAXBRANCH=&maxbranch_opt.;
        MISSING = "&assignmissing_opt.";
        BINMETHOD = "&binmethod_opt.";
        PRUNE = "&prune_opt.";
        cfg_id=_n_;
    run;
    
    /* Crear tablas para almacenar los mejores resultados */
    proc sql;
        create table BEST_DT_RESULTS (
            cfg_id num,
            MAXLEVEL num,
            NBINS num,
            LEAFSIZE num,
            MAXBRANCH num,
            CRIT char(12),
            MISSING char(12),
            BINMETHOD char,
            PRUNE char(14),
            gini_train num,
            gini_oot num
        );
    quit;    

    /* ==================================== */
    /*   CREAR LA PARTE DE PARALELIZACION   */
    /* ==================================== */

    %global dt_data_path group_act_process is_top_flg tr_sess seg_sess dt_var_seg global_bmk_path;
    %let num_sessions = 5;
    %let tr_sess = &m_troncal.;
    %let seg_sess = &m_segmento.;
    %let dt_data_path = &_root_path./Troncal_&tr_sess./Data;
    %let global_bmk_path = &_root_path./Troncal_&tr_sess./Models/Benchmark;


    %global dt_var_seg;
    %if %sysevalf(%superq(m_var_seg)=,boolean) %then %do;
        %let dt_var_seg = UNIVERSE;
    %end;
    %else %do;
        proc sql noprint;
            select distinct &m_var_seg. into :dt_var_seg from &m_caslib..full_data;
        quit;
    %end;

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
            %config_log(m_dt_task_&group_act_process., &_logs_actual);
            cas casr&group_act_process. sessopts=(caslib="&m_caslib");
            libname &m_caslib. cas caslib=&m_caslib.;
            options casdatalimit=ALL;
            %include "&_root_path/Sources/Macros/_gini.sas";
            %include "&_root_path/Sources/Macros/_get_gini_mensual.sas";
            %include "&_root_path/Sources/Modulos/m_decision_tree/__dt_train.sas";
            %include "&_root_path/Sources/Macros/_create_caslib.sas";
            %include "&_root_path/Sources/Macros/_drop_caslib.sas";            
            %__dt_train(0);
            cas casr&group_act_process. terminate;
            %Config_Log_Restore(m_dt_task_&group_act_process., &_logs_actual);
        endrsubmit;
    %end;
    /* Esperar a que todas las tareas terminen */
    waitfor _ALL_ %do group_act_process = 1 %to &num_sessions.; 
        task_&group_act_process. %end;;
    /* Cerrar sesiones paralelas */
    %do group_act_process = 1 %to &num_sessions.;
        signoff task_&group_act_process.;
    %end;

    data BEST_DT_RESULTS;
        set %do group_act_process=1 %to &num_sessions.; &m_caslib..DT_RESULTS_SESSION_&group_act_process. %end;;
    run;
    proc sort data=best_dt_results out=top_5_dt_models; by descending gini_penalizado; run;
    data &m_caslib..top_5_dt_models(copies=0 promote=yes); set top_5_dt_models(obs=5); cfg_id = _n_; run;

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
        m_castable_name=dt_results_session,
        is_loop=1
    );
    data %do group_act_process=1 %to &num_sessions.; &m_caslib..best_cfg_session_&group_act_process(copies=0 promote=yes) %end;;
        set &m_caslib..top_5_dt_models;
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
            %config_log(m_dt_model_&group_act_process., &_logs_actual);
            cas casr&group_act_process. sessopts=(caslib="&m_caslib");
            libname &m_caslib. cas caslib=&m_caslib.;
            options casdatalimit=ALL;
            %include "&_root_path/Sources/Macros/_gini.sas";
            %include "&_root_path/Sources/Macros/_calculate_gini.sas";
            %include "&_root_path/Sources/Macros/_get_gini_mensual.sas";
            %include "&_root_path/Sources/Modulos/m_decision_tree/__dt_train.sas";
            %include "&_root_path/Sources/Macros/_create_caslib.sas";
            %include "&_root_path/Sources/Macros/_drop_caslib.sas";            
            %__dt_train(1);
            cas casr&group_act_process. terminate;
            %Config_Log_Restore(m_dt_model_&group_act_process., &_logs_actual);
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
    %_merge_ginis_mensual(byvarl=&m_time., top_models=5, app_bhv_flg=&m_model_type., mlmodel= Decision Tree);
    proc sort data=&m_caslib..top_5_dt_models out=top_5_dt_models_sort; by descending gini_penalizado;run;
    title "Top 5 modelos Decision Tree por Gini Penalizado";
    proc print data=top_5_dt_models_sort noobs;run;
    title;

    %include "&_root_path/Sources/Modulos/m_champion_challenge/_save_metadata_model.sas";
    %_save_metadata_model(
        tro_var=&m_troncal.,
        seg_name=&dt_var_seg.,
        metadata_path=&dt_data_path.,
        modelabrv=dt,
        modelo_name= Decision Tree,
        segment_var =&m_var_seg.
    );

    %_drop_caslib_table(
        m_cas_session=&m_session.,
        m_caslib=&m_caslib.,
        m_castable_name=top_5_dt_models,
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
        delete _gini_mensual_bmk_train best_cfg best_dt_results bestconfiguration evaluationhistory hist_sorted top_5_dt_models: gini_: t_: report_:;
    run;

    %_drop_caslib(
        caslib_name =&m_caslib.,
        del_prom_tables = Y,
        cas_sess_name =&m_session.,
        terminate_session = Y
    );
%mend;
/*---------------------------------------------------------------------------
  Version: 2.0	  
  'Desarrollador: Joseph Chombo					
  Fecha Release: 06/10/2025
-----------------------------------------------------------------------------*/

%include "&_root_path/Sources/Modulos/m_decision_tree/dt_challenge_macro.sas";

%macro dt_report(r_train=, r_oot=, r_target=, r_time=, r_xb_pd=, r_num=, r_cat=, r_troncal=, r_segmento=, r_var_seg=, r_model_type=);

    ods graphics on / outputfmt=svg;
    /* Iniciar nuevo archivo Excel con hoja para TRAIN */
    ods html5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._dt_challenge_1.html";	
    ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._dt_challenge.xlsx"
                options(sheet_name="Benchmark TRAIN OOT" 
                    sheet_interval="none" 
                    embedded_titles="yes");
       
        %dt_challenge_macro(
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
/*---------------------------------------------------------------------------
  Version: 2.0	  
  Desarrollador: Joseph Chombo					
  Fecha Release: 06/10/2025
-----------------------------------------------------------------------------*/

%include "&_root_path/Sources/Modulos/m_decision_tree/dt_report.sas";

%macro dt_verify(v_train, v_oot, v_troncal, v_segmento);
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
        
        %dt_report(
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
        %put WARNING DEVELOPER: No se pudo ejecutar el módulo de reporte debido a errores en la validación;
    %end;
%mend;