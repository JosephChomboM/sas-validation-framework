%macro __trend_variables(t1,t2,param_target=,lista_var=,lista_var_cat=,
    groups=5);

    /* Limpiar las listas de variables para eliminar espacios extras */
    %let lista_var=%trim(&lista_var);
    %let lista_var_cat=%trim(&lista_var_cat);

    %local c v z;
    %let z=1;
    %let v_cat=%scan(&lista_var_cat., &z.," ");
    *unir variables numericas y categoricas;
    %do %while(%length(&v_cat)^=0);
        %let lista_var=&lista_var. &v_cat.#;
        %let z=%eval(&z+1);
        %let v_cat=%scan(&lista_var_cat,&z," ");
    %end;
    %let c=1;
    %let v=%scan(&lista_var, &c, " ");
    %do %while(%length(%trim(&v))>0);
        /* Usando trim para verificar si la variable es válida */
        /* Verificar que no sea un valor especial o inválido */
        %if %substr(&v, 1, 1) ne %str(.) %then %do;
            %if %substr(&v, %length(&v),1) eq %str(#) %then %do;
                %let v_aux=%substr(&v, 1, %length(&v)-1);
                %LET DUMMY_LIST=%STR(" ");
                %put "&V: ESTOY EN TRAIN DELTA 1 CORTES 0";
                %__tendencia(&t1., &v_aux, groups=5, flg_continue=0,
                    reuse_cuts=0, m_data_type=TRAIN);
                %if %sysfunc(exist(&t2.)) %then %do;
                    %PUT "&V: ESTOY EN OOT DELTA 2 CORTES 0";
                    %__tendencia(&t2., &v_aux, groups=5, flg_continue=0,
                        reuse_cuts=0, m_data_type=OOT);
                %end;
            %end;
            %else %do;
                %LET
                    DUMMY_LIST=%STR(., 1111111111, -1111111111, 2222222222, -2222222222, 3333333333, -3333333333, 4444444444, 5555555555, 
6666666666, 7777777777, -999999999);
                %put "&V: ESTOY USANDO TRAIN DATA NORMAL CORTES 0";
                %__tendencia(&t1., &v, groups=5, flg_continue=1, reuse_cuts=0,
                    m_data_type=TRAIN);
                %if %sysfunc(exist(&t2.)) %then %do;
                    %PUT "&V: ESTOY USANDO OOT DATA NORMAL CORTES 1";
                    %__tendencia(&t2., &v, groups=5, flg_continue=1,
                        reuse_cuts=1, m_data_type=OOT);
                %end;
            %end;
        %end;
        %else %do;
            %put NOTA: Saltando variable inválida: &v;
        %end;
        %let c=%eval(&c+1);
        %let v=%scan(&lista_var, &c, " ");
    %end;

    proc datasets nolist;
        delete Frecuencia CATEGORIAS CATEGORIAS_TEMP COPIA:;
    run;

%mend;

%macro __tendencia( tablain, var, groups=5, flg_continue=1, reuse_cuts=0,
    cuts_table=., report=1, m_data_type=);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    data t_&rnd._0;
        set &tablain;
        if &var in (&dummy_list) then &var=.;
            /*feature added after orig bivariates graphs was included*/
    run;

    data _null_;
        if 0 then set t_&rnd._0 nobs=n;
        call symput("total", n);
        stop;
    run;

    %if &flg_continue=1 %then %do;
        %if &reuse_cuts=0 %then %do;
            %PUT NO HAY CORTES &REUSE_CUTS;
            %_calcular_cortes(t_&rnd._0, &var, &groups);

            proc sort data=cortes;
                by rango;
            run;
        %end;
        %else %do;
            %if &cuts_table ne . %then %do;
                %if %sysfunc(exist(&cuts_table)) %then %do;
                    data cortes;
                        set &cuts_table;
                    run;
                %end;
                %else %do;
                    %PUT SI TENGO CORTES &REUSE_CUTS;
                    %_calcular_cortes(t_&rnd._0, &var, &groups);

                    proc sort data=cortes;
                        by rango;
                    run;
                %end;
            %end;
            /* if did not pass table_cuts, we assume cortes is the table we are reuse for*/
        %end;

        DATA t_&rnd._1;
            SET cortes END=EOF;
            LENGTH QUERY_START 35 QUERY_END 60;
            N=_n_;
            QUERY_START="WHEN ";
            QUERY_END="";
            IF N=1 THEN QUERY_START="CASE WHEN ";
            IF EOF THEN QUERY_END=" END";
        RUN;

        PROC SQL;
            CREATE TABLE t_&rnd._2 AS SELECT *, CASE WHEN RANGO=0 THEN
                CAT("IF MISSING(&VAR.)=1 THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                /* fixed statement to isolate missing cases bucket */ WHEN
                FLAG_INI=1 THEN
                CAT("IF &VAR.<=",FIN," AND &VAR.>. THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                WHEN FLAG_FIN=1 THEN
                CAT("IF &VAR.>",INICIO," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                ELSE
                CAT("IF ",INICIO,"<&VAR.<=",FIN," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                END AS QUERY_BODY FROM t_&rnd._1;
        QUIT;

        PROC SQL NOPRINT;
            SELECT QUERY_BODY INTO:DATAAPPLY SEPARATED BY " " FROM t_&rnd._2;
        QUIT;

        DATA t_&rnd._3;
            SET t_&rnd._0;
            LENGTH ETIQUETA 120 ;
            &DATAAPPLY.;
        RUN;

        proc sql;
            create table report as select ETIQUETA as &var., count(*) as n,
                count(*)/&total as pct_cuentas format=percent8.0,
                sum(&param_target) as defaults, mean(&param_target) as RD
                format=percent8.2 from t_&rnd._3 group by ETIQUETA ;
        quit;
    %end;

    %else %do;
        proc sql;
            create table report as select &var., /* as ETIQUETA*/ count(*) as n,
                count(*)/&total as pct_cuentas format=percent8.0,
                sum(&param_target) as defaults, mean(&param_target) as RD
                format=percent8.2 from t_&rnd._0 group by &var. ;
        quit;
    %end;

    title "Tendencia &var. - &m_data_type";

    proc sgplot data=report subpixel noautolegend;
        yaxis label="% Cuentas (bar)" discreteorder=data;
        y2axis min=0 label="RD";
        vbar &var. /response=pct_cuentas nooutline barwidth=0.4;
        vline &var. /response=rd markers markerattrs=(symbol=circlefilled)
            y2axis;
        xaxis label="Buckets variable" valueattrs=(size=8pt);
    run;
    title ;

    %if &report %then %do;
        proc print data=report noobs;
        run;
    %end;

    proc datasets nolist;
        delete t_&rnd.: report;
        run;
        %if &reuse_cuts=1 %then %do;

        proc datasets nolist;
            delete cortes;
            run;
            %end;
        %mend;

    %macro _calcular_cortes(tablain, var, groups);

        %local rnd;
        %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

        data t_&rnd._1;
            /*set &tablain(keep=&var.);*/
            set &tablain;
            &var=put(&var, F12.4);
        run;

        PROC RANK DATA=t_&rnd._1 out=t_&rnd._2 GROUPS=&groups;
            RANKS RANGO;
            VAR &var;
        RUN;

        PROC SQL;
            CREATE TABLE t_&rnd._3 AS SELECT RANGO, MIN(&var.) AS MINVAL,
                MAX(&var.) AS MAXVAL FROM t_&rnd._2 GROUP BY RANGO;
        QUIT;

        PROC SORT DATA=t_&rnd._3;
            BY RANGO;
        RUN;

        DATA t_&rnd._4;
            SET t_&rnd._3(RENAME=(RANGO=RANGO_INI)) END=EOF;
            RETAIN MARCA 0;
            N=_n_;
            FLAG_INI=0;
            FLAG_FIN=0;
            LAGMAXVAL=LAG(MAXVAL);
            RANGO=RANGO_INI+1;
            IF RANGO_INI=. THEN RANGO=0;
            IF RANGO_INI>=0 THEN MARCA=MARCA+1;
            IF MARCA=1 THEN FLAG_INI=1;
            IF EOF THEN FLAG_FIN=1;
        RUN;

        PROC SQL;
            CREATE TABLE CORTES AS SELECT "&var." AS VARIABLE LENGTH=32, RANGO,
                RANGO_INI, LAGMAXVAL AS INICIO, MAXVAL AS FIN, FLAG_INI,
                FLAG_FIN, CASE WHEN RANGO=0 THEN "00. Missing" WHEN FLAG_INI=1
                THEN CAT(PUT(RANGO,Z2.),". <-Inf; ", cats(PUT(MAXVAL,F12.4)),
                "]") WHEN FLAG_FIN=1 THEN CAT(PUT(RANGO,Z2.),". <",
                cats(PUT(LAGMAXVAL,F12.4)), "; +Inf>") ELSE CAT(PUT(RANGO,Z2.),
                ". <", cats(PUT(LAGMAXVAL,F12.4)), "; ",
                cats(PUT(MAXVAL,F12.4)), "]") END AS ETIQUETA LENGTH=200 FROM
                t_&rnd._4;
        QUIT;

        proc datasets nolist;
            delete t_&rnd.:;
        run;
    %mend;
    %include "&_root_path/Sources/Modulos/m_bivariado/bivariado_macro.sas";

%macro __bivariado_report(t1, t2=., param_target=, lista_var=., lista_var_cat=.,
    groups=5);

    ods graphics on / outputfmt=svg;
    ods html5
        file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Bivariado_1.html";
    ods excel
        file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Bivariado.xlsx"
        options(sheet_name="TRAIN - OOT" sheet_interval="none"
        embedded_titles="yes");
    %__trend_variables(&t1, &t2, param_target=&param_target,
        lista_var=&lista_var, lista_var_cat=&lista_var_cat, groups=&groups);

    ods html5 close;
    %if &exist_driver=1 %then %do;
        ods html5
            file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Bivariado_2.html";
        ods excel options(sheet_name="DRIVERS" sheet_interval="now"
            embedded_titles="yes");
        %__trend_variables(&t1, &t2, param_target=&param_target,
            lista_var=&vars_dri_num, lista_var_cat=&vars_dri_cat,
            groups=&groups);
        ODS HTML5 CLOSE;
    %end;
    ods excel close;
    ODS GRAPHICS OFF;
%mend;
%include "&_root_path/Sources/Modulos/m_bivariado/bivariado_report.sas";

%Macro verify_bivariado(dataset1, dataset2);
    %let ds1_exists=%sysfunc(exist(&dataset1));
    %let ds2_exists=%sysfunc(exist(&dataset2));
    %let target_exists=%length(&_target.) > 0;

    %let nobs1=0;
    %let nobs2=0;

    %if &ds1_exists %then %do;
        %let nobs1=%sysfunc(attrn(%sysfunc(open(&dataset1)),nobs));
        %if &nobs1=0 %then %do;
            %put WARNING: (Bivariado) Dataset &dataset1 existe con 0 obs;
        %end;
    %end;
    %else %do;
        %put WARNING: (Bivariado) Dataset &dataset1 no existe;
    %end;

    %if &ds2_exists %then %do;
        %let nobs2=%sysfunc(attrn(%sysfunc(open(&dataset2)),nobs));
        %if &nobs2=0 %then %do;
            %put WARNING: (Bivariado) Dataset &dataset2 existe con 0 obs;
        %end;
    %end;
    %else %do;
        %put WARNING: (Bivariado) Dataset &dataset2 no existe;
    %end;

    %let exist_driver=0;
    %if %length(&vars_dri_num) > 0 or %length(&vars_dri_cat) > 0 %then %do;
        %let exist_driver=1;
    %end;

    %if not &target_exists %then %do;
        %put WARNING: (Bivariado) No se encontro el target. No se puede
            ejecutar;
    %end;

    %if &target_exists and (&nobs1 > 0 or &nobs2 > 0) %then %do;
        %if &nobs1 > 0 %then %do;
            %let use_ds1=&dataset1;
        %end;
        %else %do;
            %let use_ds1=.;
        %end;

        %if &nobs2 > 0 %then %do;
            %let use_ds2=&dataset2;
        %end;
        %else %do;
            %let use_ds2=.;
        %end;

        %if %length(&vars_cat.) > 0 and %length(&vars_num.)=0 %then %do;
            %put (Bivariado) Procesando variables categoricas;
            %__bivariado_report(t1=&use_ds1, t2=&use_ds2,
                param_target=&_target., lista_var_cat=&vars_cat.);
        %end;
        %else %if %length(&vars_num.) > 0 and %length(&vars_cat.)=0 %then %do;
            %put (Bivariado) Procesando solo variables numericas;
            %__bivariado_report(t1=&use_ds1, t2=&use_ds2, param_target=&_target,
                lista_var=&vars_num, groups=5);
        %end;
        %else %if %length(&vars_cat.) > 0 and %length(&vars_num.) > 0 %then %do;
            %put (Bivariado) Procesando ambas variables numericas y categoricas;
            %__bivariado_report(t1=&use_ds1, t2=&use_ds2,
                param_target=&_target., lista_var=&vars_num.,
                lista_var_cat=&vars_cat.);
        %end;
        %else %do;
            %put WARNING: (Bivariado) No existe variables categoricas ni
                numericas;
        %end;
    %end;
    %else %do;
        %put WARNING: (Bivariado) No hay datasets validos con obs o no hay
            target;
    %end;
%Mend;
