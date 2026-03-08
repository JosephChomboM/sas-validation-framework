/* =========================================================================
missings_compute.sas - Computo de analisis de missings/dummies

Contiene macros de computo que detectan valores dummy y missing por
variable, con semaforo por umbral. Llamadas desde missings_report.sas
dentro del contexto ODS.

Macros:
%_miss_calc_var  - Calcula missings/dummies para una variable
%_miss_compute   - Orquestador: itera variables num + cat, acumula report

Valores dummy (numericos):
., 1111111111, -1111111111, 2222222222, -2222222222, 3333333333,
-3333333333, 4444444444, 5555555555, 6666666666, 7777777777, -999999999

Valores missing (categoricos):
'', 'MISSING', ' ', '.'

Usa work como staging (INSERT INTO no soportado en CAS).
Formato de imagen: JPEG.
========================================================================= */

/* =====================================================================
%_miss_calc_var - Calcula missings/dummies para una variable
Para numericas (is_numeric=1): cuenta valores en DUMMY_LIST.
Para categoricas (is_numeric=0): cuenta blancos/MISSING.
Resultado en work._miss_tmp (una fila por valor dummy encontrado).
===================================================================== */
%macro _miss_calc_var(data=, var=, is_numeric=1);

    %local _total;

    /* Obtener total de observaciones */
    proc sql noprint;
        select count(*) into :_total trimmed from &data.;
    quit;

    %if &is_numeric.=1 %then %do;
        /* Numerica: buscar valores dummy */
        proc sql noprint;
            create table work._miss_tmp as select &var. format=best16.0, "num"
                as type length 10, &_total. as total, count(*) as nmiss,
                count(*) / &_total. as pct_miss format=percent8.2 from &data.
                where &var. in (., 1111111111, -1111111111, 2222222222,
                -2222222222, 3333333333, -3333333333, 4444444444, 5555555555,
                6666666666, 7777777777, -999999999) group by &var.;
        quit;
    %end;
    %else %do;
        /* Categorica: buscar blancos y MISSING */
        proc sql noprint;
            create table work._miss_tmp as select &var., "categ" as type length
                10, &_total. as total, count(*) as nmiss, count(*) / &_total. as
                pct_miss format=percent8.2 from &data. where cats(&var.)="" or
                cats(&var.)="MISSING" or cats(&var.)=" " or cats(&var.)="."
                group by &var.;
        quit;
    %end;

    /* Detalle por valor dummy (si hay datos) */
    %local _has_rows;

    proc sql noprint;
        select count(*) into :_has_rows trimmed from work._miss_tmp;
    quit;

    %if &_has_rows. > 0 %then %do;
        proc print data=work._miss_tmp noobs;
        run;
    %end;

    /* Resumir: una fila por variable */
    proc sql noprint;
        create table work._miss_var_summary as select "&var." as Variable length
            40, max(type) as type length 10, sum(pct_miss) as total_pct_miss
            format=8.4 from work._miss_tmp;
    quit;

    proc datasets library=work nolist nowarn;
        delete _miss_tmp;
    quit;

%mend _miss_calc_var;

/* =====================================================================
%_miss_compute - Orquestador: itera variables num + cat
Acumula resultados en work._miss_report via INSERT INTO.
Genera tabla resumen con semaforo por umbral.
===================================================================== */
%macro _miss_compute(data=, vars_num=, vars_cat=, threshold=0.1);

    %local c v z v_cat;

    /* Crear formato semaforo */
    proc format;
        value MissSignif -0.0-<&threshold.="white" &threshold.-<1="red";
    run;

    /* Crear tabla acumuladora */
    data work._miss_report;
        length Variable $ 40 type $ 10 total_pct_miss 8;
        format total_pct_miss 8.4;
        stop;
    run;

    title "Missing summarize (variable/cases)";

    /* Procesar variables numericas */
    %if %length(&vars_num.) > 0 %then %do;
        %let c=1;
        %let v=%scan(&vars_num., &c., %str( ));
        %do %while(%length(&v.) > 0);
            %put NOTE: [missings] Procesando variable numerica: &v.;
            %_miss_calc_var(data=&data., var=&v., is_numeric=1);

            proc sql noprint;
                insert into work._miss_report select Variable, type,
                    total_pct_miss from work._miss_var_summary;
            quit;

            proc datasets library=work nolist nowarn;
                delete _miss_var_summary;
            quit;

            %let c=%eval(&c. + 1);
            %let v=%scan(&vars_num., &c., %str( ));
        %end;
    %end;

    /* Procesar variables categoricas */
    %if %length(&vars_cat.) > 0 %then %do;
        %let z=1;
        %let v_cat=%scan(&vars_cat., &z., %str( ));
        %do %while(%length(&v_cat.) > 0);
            %put NOTE: [missings] Procesando variable categorica: &v_cat.;
            %_miss_calc_var(data=&data., var=&v_cat., is_numeric=0);

            proc sql noprint;
                insert into work._miss_report select Variable, type,
                    total_pct_miss from work._miss_var_summary;
            quit;

            proc datasets library=work nolist nowarn;
                delete _miss_var_summary;
            quit;

            %let z=%eval(&z. + 1);
            %let v_cat=%scan(&vars_cat., &z., %str( ));
        %end;
    %end;

    title;

    /* Tabla resumen con semaforo */
    title "Missing summarize (variables)";

    proc print data=work._miss_report noobs
        style(column)={backgroundcolor=MissSignif.};
    run;
    title;

    /* Cleanup */
    proc datasets library=work nolist nowarn;
        delete _miss_report;
    quit;

%mend _miss_compute;
