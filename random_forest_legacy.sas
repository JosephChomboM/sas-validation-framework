%macro __rf_set_ranges(
    caslib_name=,
    tabla=,
    num=,
    cat=,
    target=,              /* opcional: para medir desbalance */
    event=1,              /* valor del evento positivo */
    ntrees_cap=350,       /* tope computacional recomendado (hasta 500k filas) */
    maxdepth_cap=30       /* tope de profundidad */
);
/*-------------------------------------------------------------*/
/* 1) Tamaño de muestra y conteo de variables (según su método)*/
/*-------------------------------------------------------------*/
data _null_;
    if 0 then set &caslib_name..&tabla. (where=(_partind_=1)) nobs=observations;
    call symputx('Nobs', observations);
    stop;
run;

%if %sysevalf(&Nobs <= 0) %then %do;
    %put ERROR: rf_set_ranges: tabla=&caslib_name..&tabla no tiene observaciones > 0;
    %return;
%end;

%let NumVars = %sysfunc(countw(&num, %str( )));
%put NOTE: Número de variables: &NumVars.;
%let CatVars = %sysfunc(countw(&cat, %str( )));
%put NOTE: Número de variables categoricas: &CatVars.;

/* tasa de evento (opcional) */
%global _event_rate;
%let _event_rate=.;
%if %length(&target) %then %do;
    proc sql noprint;
      select mean((&target=&event)*1) into :_event_rate trimmed
      from &caslib_name..&tabla
      where _partind_=1;
    quit;
%end;

/* Señales: tamaño (S), dimensionalidad (P), desbalance (I), mezcla categórica (C) */
%global _S _P _I _C;
%if &Nobs <= 20000 %then %let _S=1;         /* pequeño */
%else %if &Nobs <= 100000 %then %let _S=2;  /* mediano */
%else %if &Nobs <= 300000 %then %let _S=3;  /* grande */
%else %let _S=4;                            /* muy grande (hasta 500k) */

%if &NumVars <= 20 %then %let _P=1;
%else %if &NumVars <= 60 %then %let _P=2;
%else %let _P=3;

%if %sysevalf(&_event_rate=.,boolean) %then %let _I=2; /* desconocido -> moderado */
%else %if &_event_rate < 0.03 or &_event_rate > 0.97 %then %let _I=1; /* extremo */
%else %if &_event_rate < 0.10 or &_event_rate > 0.90 %then %let _I=2; /* moderado */
%else %let _I=3; /* balanceado */

%global _cat_share;
%if &NumVars>0 %then %let _cat_share=%sysevalf(&CatVars/&NumVars);
%else %let _cat_share=0;
%if &_cat_share >= 0.50 %then %let _C=1;    /* muchas categóricas */
%else %if &_cat_share >= 0.20 %then %let _C=2;
%else %let _C=3;                             /* mayoría numéricas */

/*-------------------------------------------------------------*/
/* 2) Función auxiliar para asegurar LB<=INIT<=UB              */
/*-------------------------------------------------------------*/
%macro _clamp3(name);
  %if &&&name._LB > &&&name._INIT %then %let &name._LB=&&&name._INIT;
  %if &&&name._INIT > &&&name._UB %then %let &name._INIT=&&&name._UB;
  %if &&&name._LB > &&&name._UB %then %let &name._LB=&&&name._UB;
%mend;

/*-------------------------------------------------------------*/
/* 3) Bases por tamaño (S) – valores iniciales de rangos       */
/*-------------------------------------------------------------*/
%global INBAG_LB INBAG_INIT INBAG_UB
        DEPTH_LB DEPTH_INIT DEPTH_UB
        LEAF_LB  LEAF_INIT  LEAF_UB
        NBIN_LB  NBIN_INIT  NBIN_UB
        NTREES_LB NTREES_INIT NTREES_UB
        MTRY_LB  MTRY_INIT  MTRY_UB;

%local sqrtp;
%let sqrtp = %sysfunc(max(1,%sysfunc(floor(%sysevalf(%sysfunc(sqrt(&NumVars)))))));

%if &_S=1 %then %do;           /* Nobs <= 20k */
  %let INBAG_LB = 0.70;  %let INBAG_INIT = 0.80; %let INBAG_UB = 0.90;
  %let DEPTH_LB = 6;     %let DEPTH_INIT = 12;   %let DEPTH_UB = 16;
  %let LEAF_LB  = 1;     %let LEAF_INIT  = 5;    %let LEAF_UB  = 30;
  %let NBIN_LB  = 50;    %let NBIN_INIT  = 100;  %let NBIN_UB  = 150;
  %let NTREES_LB= 80;    %let NTREES_INIT= 120;  %let NTREES_UB= 180;
  %let MTRY_LB  = %sysfunc(max(1,%sysfunc(floor(0.5*&sqrtp))));
  %let MTRY_INIT= &sqrtp;
  %let MTRY_UB  = %sysfunc(min(&NumVars,%sysfunc(floor(1.5*&sqrtp))));
%end;
%else %if &_S=2 %then %do;     /* 20k < Nobs <= 100k */
  %let INBAG_LB = 0.60;  %let INBAG_INIT = 0.70; %let INBAG_UB = 0.85;
  %let DEPTH_LB = 8;     %let DEPTH_INIT = 18;   %let DEPTH_UB = 24;
  %let LEAF_LB  = 5;     %let LEAF_INIT  = 20;   %let LEAF_UB  = 60;
  %let NBIN_LB  = 50;    %let NBIN_INIT  = 100;  %let NBIN_UB  = 200;
  %let NTREES_LB= 100;   %let NTREES_INIT= 150;  %let NTREES_UB= 250;
  %let MTRY_LB  = %sysfunc(max(1,%sysfunc(floor(0.5*&sqrtp))));
  %let MTRY_INIT= &sqrtp;
  %let MTRY_UB  = %sysfunc(min(&NumVars,%sysfunc(floor(2*&sqrtp))));
%end;
%else %if &_S=3 %then %do;     /* 100k < Nobs <= 300k */
  %let INBAG_LB = 0.50;  %let INBAG_INIT = 0.60; %let INBAG_UB = 0.70;
  %let DEPTH_LB = 10;    %let DEPTH_INIT = 20;   %let DEPTH_UB = 28;
  %let LEAF_LB  = 10;    %let LEAF_INIT  = 50;   %let LEAF_UB  = 100;
  %let NBIN_LB  = 40;    %let NBIN_INIT  = 80;   %let NBIN_UB  = 120;
  %let NTREES_LB= 150;   %let NTREES_INIT= 200;  %let NTREES_UB= 300;
  %let MTRY_LB  = %sysfunc(max(1,%sysfunc(floor(0.5*&sqrtp))));
  %let MTRY_INIT= %sysfunc(max(1,%sysfunc(floor(0.8*&sqrtp))));
  %let MTRY_UB  = %sysfunc(min(&NumVars,%sysfunc(floor(1.5*&sqrtp))));
%end;
%else %do;                    /* 300k < Nobs <= 500k */
  %let INBAG_LB = 0.50;  %let INBAG_INIT = 0.60; %let INBAG_UB = 0.65;
  %let DEPTH_LB = 10;    %let DEPTH_INIT = 18;   %let DEPTH_UB = 24;
  %let LEAF_LB  = 20;    %let LEAF_INIT  = 60;   %let LEAF_UB  = 120;
  %let NBIN_LB  = 30;    %let NBIN_INIT  = 60;   %let NBIN_UB  = 100;
  %let NTREES_LB= 200;   %let NTREES_INIT= 250;  %let NTREES_UB= 350;
  %let MTRY_LB  = %sysfunc(max(1,%sysfunc(floor(0.5*&sqrtp))));
  %let MTRY_INIT= %sysfunc(max(1,%sysfunc(floor(0.8*&sqrtp))));
  %let MTRY_UB  = %sysfunc(min(&NumVars,%sysfunc(floor(1.5*&sqrtp))));
%end;

/*-------------------------------------------------------------*/
/* 4) Ajustes por desbalance (I) y mezcla categórica (C)       */
/*-------------------------------------------------------------*/
/* Desbalance extremo: permitir hojas más pequeñas y más trees */
%if &_I=1 %then %do;
  %let LEAF_LB  = %sysfunc(max(1,%sysfunc(floor(&LEAF_LB/2))));
  %let LEAF_INIT= %sysfunc(max(1,%sysfunc(floor(&LEAF_INIT/2))));
  %let NTREES_UB= %sysfunc(min(&ntrees_cap, %eval(&NTREES_UB + 50)));
%end;

/* Muchas categóricas: subir algo minleaf y contener depth para no aislar niveles raros */
%if &_C=1 %then %do;
  %let LEAF_LB  = %sysfunc(max(&LEAF_LB,5));
  %let DEPTH_UB = %sysfunc(min(&DEPTH_UB,%eval(&maxdepth_cap-2)));
%end;

/* Dimensionalidad alta (P=3): favorecer más aleatoriedad en mtry */
%if &_P=3 %then %do;
  %let MTRY_LB  = %sysfunc(max(1,%sysfunc(floor(0.4*&sqrtp))));
  %let MTRY_INIT= %sysfunc(max(1,%sysfunc(floor(0.7*&sqrtp))));
%end;

/*-------------------------------------------------------------*/
/* 5) Clamps y orden LB<=INIT<=UB + cotas por dominio          */
/*-------------------------------------------------------------*/
/* INBAG: [0.3,1.0] */
%macro _clamp_inbag();
  %if &INBAG_LB  < 0.3 %then %let INBAG_LB=0.3;
  %if &INBAG_UB  > 1.0 %then %let INBAG_UB=1.0;
  %if &INBAG_INIT< 0.3 %then %let INBAG_INIT=0.3;
  %if &INBAG_INIT> &INBAG_UB %then %let INBAG_INIT=&INBAG_UB;
  %if &INBAG_LB  > &INBAG_INIT %then %let INBAG_LB=&INBAG_INIT;
%mend; %_clamp_inbag();

/* DEPTH: [2, maxdepth_cap] */
%let DEPTH_LB  = %sysfunc(max(2, %sysfunc(min(&maxdepth_cap,&DEPTH_LB))));
%let DEPTH_INIT= %sysfunc(max(2, %sysfunc(min(&maxdepth_cap,&DEPTH_INIT))));
%let DEPTH_UB  = %sysfunc(max(2, %sysfunc(min(&maxdepth_cap,&DEPTH_UB))));
%_clamp3(DEPTH);

/* LEAF: [1, Nobs] */
%let LEAF_LB  = %sysfunc(max(1,%sysfunc(min(&Nobs,&LEAF_LB))));
%let LEAF_INIT= %sysfunc(max(1,%sysfunc(min(&Nobs,&LEAF_INIT))));
%let LEAF_UB  = %sysfunc(max(1,%sysfunc(min(&Nobs,&LEAF_UB))));
%_clamp3(LEAF);

/* NBIN: [10,256] */
%let NBIN_LB  = %sysfunc(max(10,%sysfunc(min(256,&NBIN_LB))));
%let NBIN_INIT= %sysfunc(max(10,%sysfunc(min(256,&NBIN_INIT))));
%let NBIN_UB  = %sysfunc(max(10,%sysfunc(min(256,&NBIN_UB))));
%_clamp3(NBIN);

/* NTREES: [50, ntrees_cap] */
%let NTREES_LB  = %sysfunc(max(50,%sysfunc(min(&ntrees_cap,&NTREES_LB))));
%let NTREES_INIT= %sysfunc(max(50,%sysfunc(min(&ntrees_cap,&NTREES_INIT))));
%let NTREES_UB  = %sysfunc(max(50,%sysfunc(min(&ntrees_cap,&NTREES_UB))));
%_clamp3(NTREES);

/* MTRY: [1, NumVars] */
%let MTRY_LB  = %sysfunc(max(1,%sysfunc(min(&NumVars,&MTRY_LB))));
%let MTRY_INIT= %sysfunc(max(1,%sysfunc(min(&NumVars,&MTRY_INIT))));
%let MTRY_UB  = %sysfunc(max(1,%sysfunc(min(&NumVars,&MTRY_UB))));
%_clamp3(MTRY);

/*-------------------------------------------------------------*/
/* 6) Macrovariables de salida: bounds y opciones recomendadas */
/*-------------------------------------------------------------*/
%global inbag_bounds maxdepth_bounds minleaf_bounds numbin_bounds ntrees_bounds varstry_bounds
        assignmissing_opt binmethod_opt maxbranch_opt grow_opt info_rf;

%let inbag_bounds   = LB=&INBAG_LB   INIT=&INBAG_INIT   UB=&INBAG_UB;
%let maxdepth_bounds= LB=&DEPTH_LB   INIT=&DEPTH_INIT   UB=&DEPTH_UB;
%let minleaf_bounds = LB=&LEAF_LB    INIT=&LEAF_INIT    UB=&LEAF_UB;
%let numbin_bounds  = LB=&NBIN_LB    INIT=&NBIN_INIT    UB=&NBIN_UB;
%let ntrees_bounds  = LB=&NTREES_LB  INIT=&NTREES_INIT  UB=&NTREES_UB;
%let varstry_bounds = LB=&MTRY_LB    INIT=&MTRY_INIT    UB=&MTRY_UB;

/* Opciones SAS para mejor Gini */
%let assignmissing_opt = USEINSEARCH;
%let binmethod_opt     = QUANTILE;
%let maxbranch_opt     = 2;
%let grow_opt          = GINI;

/* Resumen para trazabilidad */
%let info_rf = tabla=&caslib_name..&tabla Nobs=&Nobs NumVars=&NumVars CatVars=&CatVars
               event_rate=%sysfunc(round(&_event_rate,0.0001))
               S=&_S P=&_P I=&_I C=&_C
               INBAG=(&inbag_bounds) DEPTH=(&maxdepth_bounds) LEAF=(&minleaf_bounds)
               NBIN=(&numbin_bounds) NTREES=(&ntrees_bounds) MTRY=(&varstry_bounds)
               OPTS=(ASSIGNMISSING=&assignmissing_opt BINMETHOD=&binmethod_opt
                     MAXBRANCH=&maxbranch_opt GROW=&grow_opt);

%put NOTE: &info_rf;
%put NOTE: BOUNDS => INBAG(&inbag_bounds) DEPTH(&maxdepth_bounds) LEAF(&minleaf_bounds)
                    NBIN(&numbin_bounds) NTREES(&ntrees_bounds) MTRY(&varstry_bounds);
%put NOTE: OPTS   => ASSIGNMISSING=&assignmissing_opt BINMETHOD=&binmethod_opt MAXBRANCH=&maxbranch_opt GROW=&grow_opt;

%mend __rf_set_ranges;
%macro __rf_train(is_top_flg);
    /* Identificadores para esta sesión */
    %let ses_p = &group_act_process.;
    %let caslib = casuser;
    %let is_top = &is_top_flg.;
    %let ses_tro = &tr_sess.;
    %let ses_seg = &seg_sess.;
    %let bmk_path = &global_bmk_path.;
    %let process_var_seg = &rf_var_seg.;
    /* Crear tabla de resultados para esta sesión */
    %put la flag de si  es top es: &is_top.;

    proc sql;
        create table RF_RESULTS_SESSION_&ses_p(
            cfg_id num,
            NTREE num,
            M num,
            BOOTSTRAP num format=best32.,
            MAXLEVEL num,
            NBINS num,
            LEAFSIZE num,
            MAXBRANCH num,
            MISSING char(12),
            BINMETHOD char,
            GROW char,
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
            call symputx('best_ntree', NTREE);
            call symputx('best_vars', M);
            call symputx('best_inbagfraction', BOOTSTRAP);
            call symputx('best_maxdepth', MAXLEVEL);
            call symputx('best_nbins', NBINS);
            call symputx('best_leafsize', LEAFSIZE);
            call symputx('best_missing', MISSING);
            call symputx('best_binmethod', BINMETHOD);
            call symputx('best_maxbranch', MAXBRANCH);
            call symputx('best_grow', GROW);
        run;
        %put [SESSION &ses_p] Se procesan los parametros para el modelo &model_id;
        %put [SESSION &ses_p] &model_id - &best_vars - &best_inbagfraction - &best_maxdepth - &best_nbins - &best_leafsize - &best_missing - &best_binmethod - &best_maxbranch - &best_grow;
        /* Entrenar el modelo rf */
        ods exclude all;
        proc forest data=&caslib..train_part
            assignmissing=&best_missing.
            binmethod=&best_binmethod.
            inbagfraction=&best_inbagfraction.
            maxbranch=&best_maxbranch.
            maxdepth=&best_maxdepth.
            minleafsize=&best_leafsize.
            ntrees=&best_ntree.
            numbin=&best_nbins.
            vars_to_try=&best_vars.
            seed=12345;
            grow &best_grow.;
            partition rolevar=_PartInd_(train='1' validate='0' test='2');
            input &m_num_inputs. / level=interval;
            %if %length(&m_cat_inputs.)>0 %then %do; input &m_cat_inputs. / level=nominal; %end;
            target &m_target. / level=nominal;
            savestate rstore=&caslib..rf_sess&ses_p._&model_id;

        run;        
        /* Luego usa PROC ASTORE */
        proc astore;
            score data=&caslib..&m_train. out=&caslib..train_scored_rf_&model_id
            rstore=&caslib..rf_sess&ses_p._&model_id copyvars=(&m_target. &m_time.);
        run;
        
        proc astore;
            score data=&caslib..&m_oot. out=&caslib..oot_scored_rf_&model_id
            rstore=&caslib..rf_sess&ses_p._&model_id copyvars=(&m_target. &m_time.);
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
                %local process_var_seg_final;
                %if %sysevalf(%superq(process_var_seg)=,boolean) %then %let process_var_seg_final=UNIVERSE;
                %else %let process_var_seg_final=&process_var_seg;

                proc casutil;
                    save casdata="rf_sess&ses_p._&model_id" incaslib="&caslib"
                            casout="&process_var_seg_final._rf" 
                            outcaslib="bmk" replace;
                quit;
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

        %_gini(&caslib., train_scored_rf_&model_id., &m_target., &predvar., g_tr);
        %_gini(&caslib., oot_scored_rf_&model_id., &m_target., &predvar., g_oot);

        %let lambda = 0.5;
        %let g_penalized = %sysevalf(&g_oot. - %sysevalf(&lambda. * %sysevalf(&g_tr. - &g_oot.)));

        %put [SESSION &ses_p] Modelo &model_id - GINI TRAIN: &g_tr - GINI OOT: &g_oot;
        
        /* Guardar resultados */
        proc sql;
            insert into RF_RESULTS_SESSION_&ses_p values(
                &model_id,
                &best_ntree,
                &best_vars,
                &best_inbagfraction,
                &best_maxdepth,
                &best_nbins,
                &best_leafsize,
                &best_maxbranch,
                "&best_missing",
                "&best_binmethod",
                "&best_grow",
                &g_tr,
                &g_oot,
                &g_penalized
            );
        quit;      
        %if &is_top. eq 1 %then %do;
            data &caslib..full_scored_rf; 
                set &caslib..train_scored_rf_&model_id &caslib..oot_scored_rf_&model_id; 
            run;
            %_get_gini_mensual(&caslib., full_scored_rf, &predvar., &m_target., &model_id., byvarl = &m_time.);
        %end;
    %end;
    data casuser.RF_RESULTS_SESSION_&ses_p(copies=0 promote=yes);
        set RF_RESULTS_SESSION_&ses_p;
    run;

    %put [SESSION &ses_p] Todos los modelos han sido procesados;

%mend __rf_train;
%macro __rf_tune(
    caslib_name=,
    tabla=,
    num_input=,
    cat_input=,
    target_input=,
    gb_stagnation=4
);
    ods exclude all;
    proc forest data=&caslib_name..&tabla.
        assignmissing=&assignmissing_opt.
        binmethod=&binmethod_opt.
        maxbranch=&maxbranch_opt.
        seed=12345;
        grow &grow_opt.;
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
                inbagFraction(&inbag_bounds.)
                maxdepth(&maxdepth_bounds.)
                minleafsize(&minleaf_bounds.)
                numbin(&numbin_bounds.)
                ntrees(&ntrees_bounds.)
                vars_to_try(&varstry_bounds.)
            );
        ods output BestConfiguration=work.bestconfiguration;
        ods output EvaluationHistory=work.evaluationhistory;        
    run;
    ods exclude none;

%mend __rf_tune;
%macro rf_challenge_macro(
    m_train=,
    m_oot=,
    m_target=,
    m_time=,
    m_xb_pd=,
    m_num_inputs=,
    m_cat_inputs=,
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
        ml_algo   = Random Forest
    );

    %include "&_root_path/Sources/Macros/_get_gini_mensual.sas";
    %_get_gini_mensual(&m_caslib., full_data, &m_xb_pd., &m_target., 0, byvarl = &m_time.);

    %include "&_root_path/Sources/Modulos/m_random_forest/__rf_set_ranges.sas";
    %__rf_set_ranges(caslib_name=&m_caslib., tabla=full_data, num=&m_num_inputs., cat=&m_cat_inputs., target=&m_target., event=1);
    
    %include "&_root_path/Sources/Modulos/m_random_forest/__rf_tune.sas";
    %__rf_tune(
        caslib_name=&m_caslib.,
        tabla=full_data,
        num_input=&m_num_inputs.,
        cat_input=&m_cat_inputs.,
        target_input=&m_target.
    );

    /* Selección de los mejores modelos */
    proc sort data=evaluationhistory out=hist_sorted; by descending GiniCoefficient; run;
    data best_cfg;
        set hist_sorted(obs=&m_top_k.);
        MISSING="&assignmissing_opt.";
        BINMETHOD="&binmethod_opt.";
        MAXBRANCH=&maxbranch_opt.;
        GROW="&grow_opt.";
        cfg_id=_n_;
    run;

    /* Crear tablas para almacenar los mejores resultados */
    proc sql;
        create table BEST_RF_RESULTS (
            cfg_id num,
            NTREE num,
            M num,
            BOOTSTRAP num format=best32.,
            MAXLEVEL num,
            NBINS num,
            LEAFSIZE num,
            MAXBRANCH num,
            MISSING char(12),
            BINMETHOD char,
            GROW char,
            gini_train num,
            gini_oot num
        );
    quit;

    /* ==================================== */
    /*   CREAR LA PARTE DE PARALELIZACION   */
    /* ==================================== */

    %global rf_data_path group_act_process is_top_flg tr_sess seg_sess rf_var_seg;
    %let num_sessions = 5;
    %let tr_sess = &m_troncal.;
    %let seg_sess = &m_segmento.;
    %let rf_data_path = &_root_path./Troncal_&tr_sess./Data;
    %let global_bmk_path = &_root_path./Troncal_&tr_sess./Models/Benchmark;
    
    
    %global rf_var_seg;
    %if %sysevalf(%superq(m_var_seg)=,boolean) %then %do;
        %let rf_var_seg = UNIVERSE;
    %end;
    %else %do;
        proc sql noprint;
            select distinct &m_var_seg. into :rf_var_seg from &m_caslib..full_data;
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
            %config_log(m_rf_task_&group_act_process.,  &_logs_actual);

            cas casr&group_act_process. sessopts=(caslib="&m_caslib");
            libname &m_caslib. cas caslib=&m_caslib.;
            options casdatalimit=ALL;
            %include "&_root_path/Sources/Macros/_gini.sas";
            %include "&_root_path/Sources/Macros/_get_gini_mensual.sas";
            %include "&_root_path/Sources/Modulos/m_random_forest/__rf_train.sas";
            %include "&_root_path/Sources/Macros/_create_caslib.sas";
            %include "&_root_path/Sources/Macros/_drop_caslib.sas";                 
            %__rf_train(0);
            cas casr&group_act_process. terminate;
            %Config_Log_Restore(m_rf_task_&group_act_process., &_logs_actual);
        endrsubmit;
    %end;
    /* Esperar a que todas las tareas terminen */
    waitfor _ALL_ %do group_act_process = 1 %to &num_sessions.; 
        task_&group_act_process. %end;;
    /* Cerrar sesiones paralelas */
    %do group_act_process = 1 %to &num_sessions.;
        signoff task_&group_act_process.;
    %end;

    data BEST_RF_RESULTS;
        set %do group_act_process=1 %to &num_sessions.; &m_caslib..RF_RESULTS_SESSION_&group_act_process. %end;;
    run;
    proc sort data=best_rf_results out=top_5_rf_models; by descending gini_penalizado; run;
    data &m_caslib..top_5_rf_models(copies=0 promote=yes); set top_5_rf_models(obs=5); cfg_id = _n_; run;

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
        m_castable_name=rf_results_session,
        is_loop=1
    );

    /* ==================================== */
    /*        ELEGIR EL TOP 5 MODELOS       */
    /* ==================================== */

    data %do group_act_process=1 %to &num_sessions.; &m_caslib..best_cfg_session_&group_act_process(copies=0 promote=yes) %end;;
        set &m_caslib..top_5_rf_models;
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

            %config_log(m_rf_model_&group_act_process.,  &_logs_actual);
            cas casr&group_act_process. sessopts=(caslib="&m_caslib");
            libname &m_caslib. cas caslib=&m_caslib.;
            options casdatalimit=ALL;
            %include "&_root_path/Sources/Macros/_gini.sas";
            %include "&_root_path/Sources/Macros/_calculate_gini.sas";
            %include "&_root_path/Sources/Macros/_get_gini_mensual.sas";
            %include "&_root_path/Sources/Modulos/m_random_forest/__rf_train.sas";
            %include "&_root_path/Sources/Macros/_create_caslib.sas";
            %include "&_root_path/Sources/Macros/_drop_caslib.sas";                 
            %__rf_train(1);
            cas casr&group_act_process. terminate;
            %Config_Log_Restore(m_rf_model_&group_act_process., &_logs_actual);
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
    %_merge_ginis_mensual(byvarl=&m_time., top_models=5, app_bhv_flg=&m_model_type., mlmodel=Random Forest);
    proc sort data=&m_caslib..top_5_rf_models out=top_5_rf_models_sort; by descending gini_penalizado;run;
    title "Top 5 modelos Random Forest por Gini Penalizado";
    proc print data=top_5_rf_models_sort noobs;run;
    title;

    %include "&_root_path/Sources/Modulos/m_champion_challenge/_save_metadata_model.sas";
    %_save_metadata_model(
        tro_var=&m_troncal.,
        seg_name=&rf_var_seg.,
        metadata_path=&rf_data_path.,
        modelabrv=rf,
        modelo_name= Random Forest,
        segment_var =&m_var_seg.
    );


    %_drop_caslib_table(
        m_cas_session=&m_session.,
        m_caslib=&m_caslib.,
        m_castable_name=top_5_rf_models,
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
        delete _gini_mensual_bmk_train best_cfg best_rf_results bestconfiguration evaluationhistory hist_sorted top_5_rf_models: gini_: t_: report_:;
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
  Desarrollador: Joseph Chombo					
  Fecha Release: 06/10/2025
-----------------------------------------------------------------------------*/

%include "&_root_path/Sources/Modulos/m_random_forest/rf_challenge_macro.sas";

%macro rf_report(r_train=, r_oot=, r_target=, r_time=, r_xb_pd=, r_num=, r_cat=, r_troncal=, r_segmento=, r_var_seg=, r_model_type=);

    ods graphics on / outputfmt=svg;
    /* Iniciar nuevo archivo Excel con hoja para TRAIN */
    ods html5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._rf_challenge_1.html";	
    ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._rf_challenge.xlsx"
                options(sheet_name="Benchmark TRAIN OOT" 
                    sheet_interval="none" 
                    embedded_titles="yes");
       
        %rf_challenge_macro(
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

%include "&_root_path/Sources/Modulos/m_random_forest/rf_report.sas";

%macro rf_verify(v_train, v_oot, v_troncal, v_segmento);
    /* Verificación de existencia de datasets */
    %let proceed = 1;
    %let train_exists = %sysfunc(exist(&v_train));
    %let oot_exists = %sysfunc(exist(&v_oot));
    
    /* Verificar existencia de datasets */
    %if &train_exists = 0 %then %do;
        %put ERROR: El dataset de entrenamiento &v_train no existe;
        %let proceed = 0;
    %end;
    %else %do;
        /* Verificar que tenga registros */
        %let train_nobs = %sysfunc(attrn(%sysfunc(open(&v_train)), NOBS));
        %if &train_nobs = 0 %then %do;
            %put ERROR: El dataset de entrenamiento &v_train existe pero no contiene registros;
            %let proceed = 0;
        %end;
        %else %do;
            %put NOTE: Dataset &v_train validado correctamente con &train_nobs registros;
        %end;
    %end;
    
    %if &oot_exists = 0 %then %do;
        %put ERROR: El dataset OOT &v_oot no existe;
        %let proceed = 0;
    %end;
    %else %do;
        /* Verificar que tenga registros */
        %let oot_nobs = %sysfunc(attrn(%sysfunc(open(&v_oot)), NOBS));
        %if &oot_nobs = 0 %then %do;
            %put ERROR: El dataset OOT &v_oot existe pero no contiene registros;
            %let proceed = 0;
        %end;
        %else %do;
            %put NOTE: Dataset &v_oot validado correctamente con &oot_nobs registros;
        %end;
    %end;
    
    /* Verificación de variables críticas */
    %if %sysevalf(&_target=,boolean) %then %do;
        %put ERROR: La variable target (&_target) está vacía;
        %let proceed = 0;
    %end;
    
    %if %sysevalf(&_var_time=,boolean) %then %do;
        %put ERROR: La variable tiempo (&_var_time) está vacía;
        %let proceed = 0;
    %end;

    %if %sysevalf(&var_pd=,boolean) %then %do;
        %put ERROR: La variable PD (&var_pd) está vacía;
        %let proceed = 0;
    %end;
    
    /* Verificar que al menos un tipo de variables (categóricas o numéricas) no esté vacío */
    %if %sysevalf(&vars_num=,boolean) and %sysevalf(&vars_cat=,boolean) %then %do;
        %put ERROR: Debe existir al menos una lista de variables categóricas o numéricas;
        %let proceed = 0;
    %end;
    
    /* Mostrar información del troncal y segmento */
    %put NOTE: Ejecutando validaciones para Troncal: &v_troncal, Segmento: &v_segmento;
    
    /* Si todas las validaciones pasan, ejecutar el reporte */
    %if &proceed = 1 %then %do;
        %put NOTE: Todas las validaciones pasaron correctamente. Ejecutando módulo de reporte...;
        
        %rf_report(
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
        %put ERROR: No se pudo ejecutar el módulo de reporte debido a errores en la validación;
    %end;
%mend;
