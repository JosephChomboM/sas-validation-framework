%macro __monotonicidad(tablain, byvar=, PD=,def=,groups=5,alias=GLOBAL,
    m_data_type=, exist_cuts=);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    data t_&rnd._0;
        set &tablain;
    run;

    data _null_;
        if 0 then set t_&rnd._0 nobs=n;
        call symput("total", n);
        stop;
    run;

    %if &exist_cuts=0 %then %do;
        %_calcular_cortes(t_&rnd._0, &PD, &groups);

        proc sort data=cortes;
            by rango;
        run;
    %end;
    %else %if &exist_cuts=1 %then %do;
        %put (MONOTONICIDAD) SE USA CORTES PREVIOS HECHOS EN EL TRAIN;
        *proc sort data=cortes;
        *by rango;
        *run;
    %end;
    %else %do;
        %put El parametro de exist_cuts debe ser 0 o 1;
        %return;
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
            CAT("IF MISSING(&PD.)=1 THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
            WHEN FLAG_INI=1 THEN
            CAT("IF &PD.<=",FIN," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";') WHEN
            FLAG_FIN=1 THEN
            CAT("IF &PD.>",INICIO," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
            ELSE
            CAT("IF ",INICIO,"<&PD.<=",FIN," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
            END AS QUERY_BODY FROM t_&rnd._1;
    QUIT;

    PROC SQL NOPRINT;
        SELECT QUERY_BODY INTO:DATAAPPLY SEPARATED BY " " FROM t_&rnd._2;
    QUIT;

    DATA t_&rnd._3;
        SET t_&rnd._0;
        LENGTH ETIQUETA 120;
        *&PD = put(&PD, comma12.6);
        &DATAAPPLY.;
    RUN;

    proc sql;
        create table report as select ETIQUETA, count(*) as Cuentas,
            count(*)/&total as Pct_cuentas format=percent8.2, mean(&def) as
            Mean_&def. format=percent8.2 from t_&rnd._3 group by etiqueta order
            by etiqueta asc ;
    quit;

    title "Granulado Score &ALIAS - &m_data_type";

    proc sgplot data=report;
        keylegend / title=" " opaque;
        vbar ETIQUETA /response=Pct_cuentas BARWIDTH=.4 NOOUTLINE;
        vline ETIQUETA /response=Mean_&def. markers
            markerattrs=(symbol=circlefilled) y2axis;
        yaxis label="% Cuentas (bar)" discreteorder=data labelattrs=(size=8)
            valueattrs=(size=8);
        y2axis min=0 label="Mean &def" labelattrs=(size=8);
        xaxis label="Buckets &PD" labelattrs=(size=8);
    run;
    title ;

    proc print data=report noobs;
    run;

    proc datasets nolist;
        delete report t_&rnd.:;
        run;
        %if &exist_cuts=1 %then %do;

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
                cats(PUT(MAXVAL,F12.4)), "]")
                /*
WHEN FLAG_INI=1 THEN CAT(PUT(RANGO,Z2.),". <-Inf; ", cats(MAXVAL), "]")
	             WHEN FLAG_FIN=1 THEN CAT(PUT(RANGO,Z2.),". <", cats(LAGMAXVAL), "; +Inf>")
	             ELSE CAT(PUT(RANGO,Z2.), ". <", cats(LAGMAXVAL), "; ", cats(MAXVAL), "]") */
                END AS ETIQUETA LENGTH=200 FROM t_&rnd._4;
        QUIT;

        proc datasets nolist;
            delete t_&rnd.:;
        run;

    %mend;
    /*---------------------------------------------------------------------------
    Version: 2.0
    Desarrollador: Joseph Chombo
    Fecha Release: 01/09/2025
    -----------------------------------------------------------------------------*/
    %include
        "&_root_path/Sources/Modulos/m_monotonicidad/monotonicidad_macro.sas";

%macro __monotonicidad_report(dataset=, byvar=, PD=, def=, groups=5,
    data_type=);

    ods graphics on / outputfmt=svg;

    %if &data_type=TRAIN %then %do;
        /* Iniciar nuevo archivo Excel con hoja para TRAIN */
        ods html5
            file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Monotonicidad_1.html";
        ods excel
            file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Monotonicidad.xlsx"
            options(sheet_name="TRAIN_Monotonicidad" sheet_interval="none"
            embedded_titles="yes");

        /* Ejecutar análisis de monotonicidad para TRAIN */
        %__monotonicidad(tablain=&dataset, byvar=&byvar, PD=&PD, def=&def,
            groups=&groups, m_data_type=&data_type ,exist_cuts=0);

        ods html5 close;
    %end;
    %else %if &data_type=OOT %then %do;
        /* Agregar hoja OOT al mismo archivo */
        ods HTML5
            file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Monotonicidad_2.html";
        ods excel options(sheet_name="OOT_Monotonicidad" sheet_interval="now"
            embedded_titles="yes");

        /* Ejecutar análisis de monotonicidad para OOT */
        %__monotonicidad(tablain=&dataset, byvar=&byvar, PD=&PD, def=&def,
            groups=&groups, m_data_type=&data_type, exist_cuts=1);
        ods excel close;
        ods html5 close;

    %end;

    ods graphics off;

%mend;
/*---------------------------------------------------------------------------
Version: 1.0
Desarrollador: Joseph Chombo
Fecha Release: 04/02/2025
-----------------------------------------------------------------------------*/
%include "&_root_path/Sources/Modulos/m_monotonicidad/monotonicidad_report.sas";

%macro verify_monotonicidad(dataset, data_type=);
    %if %sysfunc(exist(&dataset.)) and %length(&_var_time) > 0 and
        %length(&var_pd) > 0 and %length(&_target) > 0 %then %do;
        %__monotonicidad_report(dataset=&dataset, byvar=&_var_time, PD=&var_pd,
            def=&_target, groups=5, data_type=&data_type);
    %end;
    %else %do;
        %put (Monotonicidad) No se pudo ejecutar porque falta alguno de:
            dataset, byvar (_var_time), PD (var_pd) o def (_target);
    %end;
%mend;
