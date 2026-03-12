/*--------------------------------------------------------------
Macro: __gini_variable_m
Descripción: Calcula GINI individual para cada variable predictora
Output: gini_variables, gini_variables_comp
--------------------------------------------------------------*/
%include "&_root_path./Sources/Modulos/m_gini/__aux_gini_utils.sas";

%macro __gini_variable_m( 
    t1=, /* Dataset TRAIN */
    t2=, /* Dataset OOT */
    target=, /* Variable target (0/1) */ 
    var_list=, /* Lista de variables a evaluar (separadas por espacio) */
    param_model_type=APP /* APP o BHV para umbrales */
);

    %local rnd thres_rojo thres_amarillo n_vars i var;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    /* Definir umbrales según tipo de modelo */
    %if &param_model_type.=BHV %then %do;
        %let thres_rojo=0.50;
        %let thres_amarillo=0.60;
    %end;
    %else %do;
        %let thres_rojo=0.40;
        %let thres_amarillo=0.50;
    %end;

    /* Contar variables */
    %let n_vars=%sysfunc(countw(&var_list., %str( )));

    /* Inicializar tabla resultado */
    proc sql;
        create table gini_variables ( Variable char(64), Dataset char(10), N_Obs
            num, N_Valid num, Pct_Valid num format=percent8.2, N_Default num,
            Gini num format=8.4, Ranking num );
    quit;

    /*=========================================
    1. Calcular GINI por variable - TRAIN
    =========================================*/
    %if %sysfunc(exist(&t1.)) %then %do;
        %put NOTE: [GINI VARIABLE] Procesando TRAIN con &n_vars. variables;

        %do i=1 %to &n_vars.;
            %let var=%scan(&var_list., &i., %str( ));

            /* Verificar que la variable existe */
            %let dsid=%sysfunc(open(&t1.));
            %let varnum=%sysfunc(varnum(&dsid., &var.));
            %let rc=%sysfunc(close(&dsid.));

            %if &varnum. > 0 %then %do;
                /* Estadísticos básicos */
                proc sql noprint;
                    select count(*), sum(case when &var. is not missing then 1
                        else 0 end), sum(&target.) into :n_obs trimmed, :n_valid
                        trimmed, :n_def trimmed from &t1.;
                quit;

                /* Calcular GINI solo para registros válidos */
                %if &n_valid. >= 30 %then %do;
                    data _temp_train_&i.;
                        set &t1.;
                        where &var. is not missing;
                    run;

                    %__gini_calc( data=_temp_train_&i., target=&target.,
                        var=&var., out_gini=_gini_var_val );

                    /* Calcular porcentaje */
                    %let pct_valid=%sysevalf(&n_valid. / &n_obs.);

                    /* Insertar resultado */
                    proc sql;
                        insert into gini_variables values ( "&var.", "TRAIN",
                            &n_obs., &n_valid., &pct_valid., &n_def.,
                            &_gini_var_val., . );
                    quit;

                    proc datasets lib=work nolist;
                        delete _temp_train_&i.;
                    quit;
                %end;
                %else %do;
                    /* Variable con pocos datos válidos */
                    %let pct_valid=%sysevalf(&n_valid. / &n_obs.);

                    proc sql;
                        insert into gini_variables values ( "&var.", "TRAIN",
                            &n_obs., &n_valid., &pct_valid., &n_def., ., . );
                    quit;
                %end;
            %end;
            %else %do;
                %put WARNING: [GINI VARIABLE] Variable &var. no existe en TRAIN;
            %end;
        %end;
    %end;

    /*=========================================
    2. Calcular GINI por variable - OOT
    =========================================*/
    %if %sysfunc(exist(&t2.)) %then %do;
        %put NOTE: [GINI VARIABLE] Procesando OOT con &n_vars. variables;

        %do i=1 %to &n_vars.;
            %let var=%scan(&var_list., &i., %str( ));

            /* Verificar que la variable existe */
            %let dsid=%sysfunc(open(&t2.));
            %let varnum=%sysfunc(varnum(&dsid., &var.));
            %let rc=%sysfunc(close(&dsid.));

            %if &varnum. > 0 %then %do;
                /* Estadísticos básicos */
                proc sql noprint;
                    select count(*), sum(case when &var. is not missing then 1
                        else 0 end), sum(&target.) into :n_obs trimmed, :n_valid
                        trimmed, :n_def trimmed from &t2.;
                quit;

                /* Calcular GINI solo para registros válidos */
                %if &n_valid. >= 30 %then %do;
                    data _temp_oot_&i.;
                        set &t2.;
                        where &var. is not missing;
                    run;

                    %__gini_calc( data=_temp_oot_&i., target=&target.,
                        var=&var., out_gini=_gini_var_val );

                    /* Calcular porcentaje */
                    %let pct_valid=%sysevalf(&n_valid. / &n_obs.);

                    /* Insertar resultado */
                    proc sql;
                        insert into gini_variables values ( "&var.", "OOT",
                            &n_obs., &n_valid., &pct_valid., &n_def.,
                            &_gini_var_val., . );
                    quit;

                    proc datasets lib=work nolist;
                        delete _temp_oot_&i.;
                    quit;
                %end;
                %else %do;
                    /* Variable con pocos datos válidos */
                    %let pct_valid=%sysevalf(&n_valid. / &n_obs.);

                    proc sql;
                        insert into gini_variables values ( "&var.", "OOT",
                            &n_obs., &n_valid., &pct_valid., &n_def., ., . );
                    quit;
                %end;
            %end;
            %else %do;
                %put WARNING: [GINI VARIABLE] Variable &var. no existe en OOT;
            %end;
        %end;
    %end;

    /*=========================================
    3. Calcular Ranking por GINI
    =========================================*/
    proc sql;
        create table _ranking_train as select Variable, Dataset, N_Obs, N_Valid,
            Pct_Valid, N_Default, Gini, monotonic() as Ranking from
            gini_variables where Dataset="TRAIN" order by Gini descending;

        create table _ranking_oot as select Variable, Dataset, N_Obs, N_Valid,
            Pct_Valid, N_Default, Gini, monotonic() as Ranking from
            gini_variables where Dataset="OOT" order by Gini descending;
    quit;

    /* Reconstruir tabla con rankings */
    data gini_variables;
        set _ranking_train _ranking_oot;
    run;

    /*=========================================
    4. Crear tabla comparativa TRAIN vs OOT
    =========================================*/
    proc sql;
        create table gini_variables_comp as select t.Variable, t.Gini as
            Gini_Train format=8.4, t.Ranking as Rank_Train, o.Gini as Gini_OOT
            format=8.4, o.Ranking as Rank_OOT, (t.Gini - o.Gini) as Delta_Gini
            format=8.4, abs(t.Ranking - o.Ranking) as Delta_Rank, case when
            calculated Delta_Gini < -0.05 then "DEGRADACION" when calculated
            Delta_Gini > 0.05 then "MEJORA" else "ESTABLE" end as Estabilidad
            length=15 from gini_variables t left join gini_variables o on
            t.Variable=o.Variable and o.Dataset="OOT" where t.Dataset="TRAIN"
            order by t.Ranking;
    quit;

    /* Limpieza */
    proc datasets lib=work nolist;
        delete _ranking_train _ranking_oot;
    quit;

    %put NOTE: [GINI VARIABLE] Cálculo completado para &n_vars. variables;

%mend __gini_variable_m;

/*--------------------------------------------------------------
Macro: __gini_variable_mensual_m
Descripción: Calcula GINI por variable y periodo (CUBO completo)
Output: cubo_gini_variables, cubo_gini_resumen
--------------------------------------------------------------*/

%macro __gini_variable_mensual_m(
    t1=, /* Dataset TRAIN */ 
    t2=, /* Dataset OOT */ 
    target=, /* Variable target (0/1) */ 
    var_list=, /* Lista de variables a evaluar */ 
    time_var=, /* Variable temporal (YYYYMM numerico) */
    param_model_type=APP, /* APP o BHV para umbrales */ 
    use_weights=1, /* 1=usar ponderación balanceada, 0=sin pesos */
    def_close=0 /* Meses de cierre de default (filtro temporal) */
);

    %local rnd n_vars i var;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));
    %let n_vars=%sysfunc(countw(&var_list., %str( )));

    /* Inicializar CUBO resultado (formato largo) */
    proc sql;
        create table cubo_gini_variables ( Variable char(64), Dataset char(10),
            Periodo num format=6., N num, N_Valid num, N_Default num, Estimate
            num format=12.6, WaldChiSq num format=10.4, ProbChiSq num
            format=8.6, Gini num format=8.4 );
    quit;

    /*=========================================
    1. Procesar cada variable - TRAIN y OOT
    =========================================*/
    %do i=1 %to &n_vars.;
        %let var=%scan(&var_list., &i., %str( ));

        %if %sysfunc(exist(&t1.)) %then %do;
            %__gini_var_all_periods( data=&t1., target=&target., var=&var.,
                time_var=&time_var., dataset_name=TRAIN,
                use_weights=&use_weights., def_close=&def_close. );
        %end;

        %if %sysfunc(exist(&t2.)) %then %do;
            %__gini_var_all_periods( data=&t2., target=&target., var=&var.,
                time_var=&time_var., dataset_name=OOT,
                use_weights=&use_weights., def_close=&def_close. );
        %end;
    %end;

    /*=========================================
    2. Ordenar CUBO: Variable ASC, Dataset, Periodo ASC
    =========================================*/
    proc sort data=cubo_gini_variables;
        by Variable Dataset Periodo;
    run;

    /*=========================================
    3. Calcular resumen por variable
    =========================================*/
    proc sql;
        create table cubo_gini_resumen as
        select a.Variable, a.Dataset,
            a.N_Periodos, a.First_Period format=6., 
            a.Last_Period format=6.,
            first.Gini as Gini_First format=8.4, 
            last.Gini as Gini_Last format=8.4, 
            a.Gini_Promedio format=8.4, 
            a.Gini_Min format=8.4,
            a.Gini_Max format=8.4, 
            a.Gini_Std format=8.4, 
            (last.Gini - first.Gini) as Delta_Gini format=8.4,
            case
                when last.Gini is missing or first.Gini is missing then "SIN DATOS"
                when (last.Gini - first.Gini) < -0.03 then "EMPEORANDO"
                when (last.Gini - first.Gini) > 0.03 then "MEJORANDO" else "ESTABLE" end as Tendencia length=15,
            case 
                when a.Gini_Promedio >= 0.15 then "SATISFACTORIO" 
                when a.Gini_Promedio >= 0.05 then "ACEPTABLE" else "BAJO" end as Evaluacion length=15 
        from(
            select 
                Variable, 
                Dataset, 
                count(*) as N_Periodos, 
                min(Periodo) as First_Period, 
                max(Periodo) as Last_Period, 
                mean(Gini) as Gini_Promedio, 
                min(Gini) as Gini_Min,
                max(Gini) as Gini_Max, 
                std(Gini) as Gini_Std 
            from cubo_gini_variables 
            where Gini is not missing
            group by Variable, Dataset
        ) a left join cubo_gini_variables first on a.Variable=
            first.Variable and a.Dataset=first.Dataset and a.First_Period=
            first.Periodo left join cubo_gini_variables last on a.Variable=
            last.Variable and a.Dataset=last.Dataset and a.Last_Period=
            last.Periodo order by a.Variable, a.Dataset;
    quit;

    %put NOTE: [GINI VARIABLE MENSUAL] CUBO completado: &n_vars. variables
        procesadas;

%mend __gini_variable_mensual_m;