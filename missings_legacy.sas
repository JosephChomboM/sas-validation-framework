/*---------------------------------------------------------------------------
Version: 2.0
Desarrollador: Joseph Chombo
Fecha Release: 01/09/2025
-----------------------------------------------------------------------------*/
%macro __get_missings(table, var, flg_continue=1);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    data t_&rnd._1;
        set &table;
    run;

    data _null_;
        if 0 then set t_&rnd._1 nobs=n;
        call symputx("total", n);
        stop;
    run;

    %put TOTAL &=&total;

    %if &flg_continue %then %do;
        proc sql;
            create table reporte_miss as select &var. format=best16.0, "num" as
                type, &total as total, count(*) as nmiss, count(*)/&total as
                pct_miss format=percent8.2 from t_&rnd._1 where &var. in
                (&dummy_list.) group by &var. ;
        quit;

        proc print data=reporte_miss noobs;
        run;

    %end;

    %else %do;
        proc sql;
            create table reporte_miss as select &var., "categ" as type, &total
                as total, count(*) as nmiss, count(*)/&total as pct_miss
                format=percent8.2 from t_&rnd._1 where cats(&var)="" or
                cats(&var)="MISSING" or cats(&var)=" " or cats(&var)="." group
                by &var. ;
        quit;

        proc print data=reporte_miss noobs;
        run;
    %end;

    proc sql;
        create table tmp as
            /* does not drop because it will use in get_missings_variables macro*/
            select variable, type, total_pct_miss from (select "&var" as
            variable, max(type) as type, sum(pct_miss) as total_pct_miss from
            reporte_miss) x ;
    quit;

    proc datasets nolist;
        delete reporte_miss t_&rnd.:;
    run;

%mend;

%macro __get_missings_variables(table,
    lista_var=,lista_var_cat=,Thres_missing=1);
    *Calcula el tipo de missing de las variables;
    *Calcula el nivel de missing de las variables;
    %local c v z;

    %LET
        DUMMY_LIST=%STR(., 1111111111, -1111111111, 2222222222, -2222222222, 3333333333, -3333333333, 4444444444, 5555555555, 
    6666666666, 7777777777, -999999999);

    %let c=1;
    %let v=%scan(&lista_var, &c, " ");
    %global lista_miss_dev lista_miss_oot lista_type_dev lista_type_oot;

    data report_m;
        length Variable 40 type 10 total_pct_missing 8.;
        format Variable char40. type char10. total_pct_missing 8.4;
        stop;
    run;

    proc format;
        value MissSignif -0.0-<&THRES_MISSING="white" &THRES_MISSING-<1="red" ;
    run;

    title Missing summarize (variable/cases);

    /* Procesa variables numéricas */
    %if %length(&lista_var) > 0 %then %do;
        %let c=1;
        %let v=%scan(&lista_var, &c, " ");
        %do %while(%length(&v)^=0);
            %put Processing numeric variable: &v;
            %__get_missings(&table, &v, flg_continue=1);
                /* flg_continue=1 para numéricas */

            proc sql;
                insert into report_m select Variable, type, total_pct_miss from
                    tmp;
            quit;

            %let c=%eval(&c+1);
            %let v=%scan(&lista_var, &c, " ");

            proc datasets nolist;
                delete tmp;
                run;
                %end;
                %end;

                /* Procesa variables categóricas */
                %if %length(&lista_var_cat) > 0 %then %do;
                    %let z=1;
                    %let v_cat=%scan(&lista_var_cat., &z., " ");
                    %do %while(%length(&v_cat)^=0);
                        %put Processing categorical variable: &v_cat;
                        %__get_missings(&table, &v_cat, flg_continue=0);
                            /* flg_continue=0 para categóricas */

                    proc sql;
                        insert into report_m select Variable, type,
                            total_pct_miss from tmp;
                    quit;

                    %let z=%eval(&z+1);
                    %let v_cat=%scan(&lista_var_cat, &z, " ");

                proc datasets nolist;
                    delete tmp;
                    run;
                    %end;
                    %end;

                    title ;

                    title Missing summarize (variables);

            proc print data=report_m style(column)={backgroundcolor=MissSignif.}
                noobs;
            run;
            title ;

            proc sql noprint;
                select variable into: lista_miss separated by ' ' from report_m
                    where total_pct_missing>&THRES_MISSING;
                select type into: lista_type separated by ' ' from report_m
                    where total_pct_missing>&THRES_MISSING;
            quit;

            proc datasets nolist;
                delete report_m;
            run;

            %mend;
            /*---------------------------------------------------------------------------
            Version: 2.0
            Desarrollador: Joseph Chombo
            Fecha Release: 01/09/2025
            -----------------------------------------------------------------------------*/
            %include "&_root_path/Sources/Modulos/m_missing/missing_macro.sas";

        %macro __missing_report(data=, lista_var=, lista_var_cat=, threshold=,
            data_type=);

            ods graphics on / outputfmt=svg;
            %if &data_type=TRAIN %then %do;
                /* Iniciar nuevo archivo Excel con hoja para TRAIN */
                ods html5
                    file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Missing_1.html";
                ods excel
                    file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Missing.xlsx"
                    options(sheet_name="TRAIN_Missing" sheet_interval="none"
                    embedded_titles="yes");

                /* Título para el reporte TRAIN */
                title "TRAIN: Análisis de Missings";

                /* Ejecutar análisis de missings para TRAIN */
                %__get_missings_variables(&data, %if %length(&lista_var) > 0
                    %then %do;
            lista_var=&lista_var, %end;
            %if %length(&lista_var_cat) > 0 %then %do;
            lista_var_cat=&lista_var_cat, %end;
            Thres_missing=&threshold);
            title;
            ods html5 close;
            %end;
            %else %if &data_type=OOT %then %do;
                /* Agregar hoja OOT al mismo archivo */
                ods html5
                    file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Missing_2.html";
                ods excel options(sheet_name="OOT_Missing" sheet_interval="now"
                    embedded_titles="yes");

                /* Título para el reporte OOT */
                title "OOT: Análisis de Missings";

                /* Ejecutar análisis de missings para OOT */
                %__get_missings_variables(&data, %if %length(&lista_var) > 0
                    %then %do;
            lista_var=&lista_var, %end;
            %if %length(&lista_var_cat) > 0 %then %do;
            lista_var_cat=&lista_var_cat, %end;
            Thres_missing=&threshold);
            title;
            ods excel close;
            ods html5 close;
            %end;
            ods graphics off;
        %mend;
        /*---------------------------------------------------------------------------
        Version: 2.0
        Desarrollador: Joseph Chombo
        Fecha Release: 01/09/2025
        -----------------------------------------------------------------------------*/
        %include "&_root_path/Sources/Modulos/m_missing/missing_report.sas";

    %macro verify_missings(dataset, data_type=);
        %if %sysfunc(exist(&dataset)) %then %do;
            %if %length(&vars_cat.) > 0 or %length(&vars_num.) > 0 %then %do;
                %let v_thresh=%sysfunc(ifc(%length(&_thresh)=0, 0.1, &_thresh));
                %if %length(&vars_cat.) > 0 and %length(&vars_num.) > 0 %then
                    %do;
                    %__missing_report(data=&dataset, lista_var=&vars_num,
                        lista_var_cat=&vars_cat, threshold=&v_thresh,
                        data_type=&data_type);
                %end;
                %else %if %length(&vars_cat.) > 0 %then %do;
                    %__missing_report(data=&dataset, lista_var_cat=&vars_cat,
                        threshold=&v_thresh, data_type=&data_type);
                %end;
                %else %if %length(&vars_num.) > 0 %then %do;
                    %__missing_report(data=&dataset, lista_var=&vars_num,
                        threshold=&v_thresh, data_type=&data_type);
                %end;
            %end;
            %else %do;
                %put WARNING DEVELOPER: (Missing) No existen variables numéricas
                    ni categóricas;
            %end;
        %end;
        %else %do;
            %put WARNING DEVELOPER: (Missing) No existe el dataset en el entorno
                SAS;
        %end;
    %mend;
