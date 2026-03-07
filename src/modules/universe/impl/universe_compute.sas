/* =========================================================================
universe_compute.sas - Cómputo de análisis descriptivo del universo

Contiene macros de cómputo que procesan las tablas y generan resultados
intermedios en casuser (CAS). Estas macros son llamadas desde
universe_report.sas dentro del contexto ODS.

Macros:
%_univ_describe_id      - Evolutivo cuentas + verificación duplicados
%_univ_bandas_cuentas   - Bandas ±2σ sobre cuentas (sin duplicados)
%_univ_evolutivo_monto  - Suma de monto por periodo (tabla + gráfico)
%_univ_describe_monto   - Media de monto por periodo (gráfico)

Las bandas se calculan desde TRAIN (is_train=1) y se aplican a OOT
(is_train=0) usando las macro variables globales &_univ_mean y &_univ_std.

Tablas temporales se crean en casuser (CAS).
========================================================================= */

/* =====================================================================
%_univ_describe_id - Evolutivo de cuentas + duplicados
Gráfico de barras de registros por periodo + tabla de duplicados
===================================================================== */
%macro _univ_describe_id(data=, byvar=, id_var=);

    title "Evolutivo Cuentas - &data.";

    proc freq data=&data.;
        tables &byvar. / out=casuser._univ_evolut_cuenta;
    run;

    proc sgplot data=casuser._univ_evolut_cuenta;
        vbar &byvar. / response=Count NOOUTLINE FILLATTRS=(color=LIGHTSTEELBLUE)
            barwidth=0.4;
        yaxis label="Cuentas" min=0;
        xaxis label="&byvar.";
    run;

    /* Detección de duplicados */
    proc sql noprint;
        create table casuser._univ_dup as select &byvar., &id_var., count(*) as
            N from &data. group by &byvar., &id_var. having N > 1;
    quit;

    title;

    proc datasets library=casuser nolist nowarn;
        delete _univ_evolut_cuenta _univ_dup;
    quit;

%mend _univ_describe_id;

/* =====================================================================
%_univ_bandas_cuentas - Bandas ±2σ sobre cuentas (sin duplicados)
Calcula mean/std desde TRAIN (is_train=1) y los guarda en macrovars
globales. En OOT (is_train=0) los reutiliza y luego los resetea.
===================================================================== */
%macro _univ_bandas_cuentas(data=, byvar=, id_var=, is_train=1);

    %global _univ_mean _univ_std;

    /* Eliminar duplicados por periodo + id */
    proc sort data=&data. nodupkey out=casuser._univ_sindup;
        by &byvar. &id_var.;
    run;

    proc freq data=casuser._univ_sindup;
        tables &byvar. / out=casuser._univ_freq_cuentas;
    run;

    /* Calcular mean/std solo desde TRAIN */
    %if &is_train.=1 %then %do;
        proc sql noprint;
            select mean(Count) into :_univ_mean trimmed from
                casuser._univ_freq_cuentas;

            select std(Count) into :_univ_std trimmed from
                casuser._univ_freq_cuentas;
        quit;
        %put NOTE: [univ_bandas] TRAIN: mean=&_univ_mean. std=&_univ_std.;
    %end;

    /* Calcular límites de bandas */
    %local inf sup max_val;
    %let inf=%sysevalf(&_univ_mean. - 2 * &_univ_std.);
    %let sup=%sysevalf(&_univ_mean. + 2 * &_univ_std.);
    %let max_val=%sysevalf(&_univ_mean. + 3 * &_univ_std.);

    title "Evolutivo Cuentas (±2σ) - &data.";

    proc sgplot data=casuser._univ_freq_cuentas subpixel noautolegend;
        band x=&byvar. lower=&inf. upper=&sup. / fillattrs=(color=graydd)
            legendlabel="± 2 Desv. Estandar" name="band1";
        series x=&byvar. y=Count / markers lineattrs=(color=black thickness=2)
            legendlabel="Cuentas" name="serie1";
        refline &_univ_mean. / lineattrs=(color=red pattern=Dash)
            legendlabel="Overall Mean" name="line1";
        yaxis min=0 max=&max_val. label="Promedio de Cuentas";
        xaxis label="&byvar." type=discrete;
        keylegend "serie1" "band1" / location=inside position=bottomright;
    run;

    title;

    /* Resetear globales después de OOT para evitar leaks */
    %if &is_train.=0 %then %do;
        %let _univ_mean=0;
        %let _univ_std=0;
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _univ_sindup _univ_freq_cuentas;
    quit;

%mend _univ_bandas_cuentas;

/* =====================================================================
%_univ_evolutivo_monto - Suma de monto por periodo (barras + tabla)
===================================================================== */
%macro _univ_evolutivo_monto(data=, monto_var=, byvar=);

    proc sql;
        create table casuser._univ_sum_monto as select &byvar., sum(&monto_var.)
            as Sum_Monto from &data. group by &byvar.;
    quit;

    proc sort data=casuser._univ_sum_monto;
        by &byvar.;
    run;

    title "Suma &monto_var. por &byvar.";

    proc sgplot data=casuser._univ_sum_monto;
        vbar &byvar. / response=Sum_Monto barwidth=1;
        xaxis label="&byvar.";
        yaxis label="&monto_var.";
    run;

    proc print data=casuser._univ_sum_monto noobs;
    run;

    title;

    proc datasets library=casuser nolist nowarn;
        delete _univ_sum_monto;
    quit;

%mend _univ_evolutivo_monto;

/* =====================================================================
%_univ_describe_monto - Media de monto por periodo (línea)
===================================================================== */
%macro _univ_describe_monto(data=, monto_var=, byvar=);

    proc means data=&data. n mean nonobs;
        var &monto_var.;
        class &byvar.;
        output out=casuser._univ_evolut_monto n=N mean=Mean;
    run;

    data casuser._univ_evolut_monto2;
        set casuser._univ_evolut_monto;
        where _TYPE_ ne 0;
    run;

    title "Evolutivo &monto_var.";

    proc sgplot data=casuser._univ_evolut_monto2;
        vline &byvar. / response=Mean markers markerattrs=(symbol=circlefilled
            color=black) lineattrs=(color=crimson);
        yaxis label="mean &monto_var." valuesformat=COMMA16.0 min=0;
    run;

    title;

    proc datasets library=casuser nolist nowarn;
        delete _univ_evolut_monto _univ_evolut_monto2;
    quit;

%mend _univ_describe_monto;
