/* =========================================================================
target_compute.sas - Computo de analisis del target (ratio de default)

Contiene macros de computo que procesan las tablas y generan resultados
intermedios en casuser (CAS). Estas macros son llamadas desde
target_report.sas dentro del contexto ODS.

Macros:
%_target_describe       - Evolutivo RD + materialidad + diferencia relativa
%_target_bandas         - Bandas +/-2s sobre RD (TRAIN -> OOT)
%_target_ponderado_promedio - RD ponderado por monto (promedio)
%_target_ponderado_suma     - RD ponderado por monto (suma) + ratio

Las bandas se calculan desde TRAIN (is_train=1) y se aplican a OOT
(is_train=0) usando las macro variables globales correspondientes.

Tablas temporales se crean en casuser (CAS) via PROC FEDSQL.
Formato de imagen: JPEG. def_cld filtra default cerrado.
========================================================================= */

/* =====================================================================
%_target_describe - Evolutivo de RD + materialidad + diferencia relativa
Calcula mean(target) por periodo, con filtro opcional por def_cld.
Genera grafico evolutivo + tabla de materialidad + diferencia relativa.
===================================================================== */
%macro _target_describe(data=, target=, byvar=, def_cld=0);

    %local num_months;

    title "Evolutivo &target. - &data.";

    /* Filtrar por default cerrado si se indico */
    %if &def_cld. ne 0 %then %do;
        %put NOTE: [target_describe] Default cerrado hasta &def_cld.;

        proc fedsql sessref=conn noprint;
            create table casuser._tgt_data_filt {options replace=true} as select
                * from &data. where &byvar. <= &def_cld.;
        quit;
    %end;
    %else %do;
        proc fedsql sessref=conn noprint;
            create table casuser._tgt_data_filt {options replace=true} as select
                * from &data.;
        quit;
    %end;

    /* Calcular mean(target) por periodo via FEDSQL */
    proc fedsql sessref=conn noprint;
        create table casuser._tgt_evolut_target {options replace=true} as select
            &byvar., count(*) as N, avg(&target.) as Mean from
            casuser._tgt_data_filt group by &byvar.;
    quit;

    /* Contar meses */
    proc sql noprint;
        select count(*) into :num_months trimmed from
            casuser._tgt_evolut_target;
    quit;

    /* Diferencia relativa: primeros vs ultimos meses */
    %if &num_months. ne 1 %then %do;

        %local first_mean last_mean relative_diff n_compare;

        %if &num_months. >= 6 %then %let n_compare=3;
        %else %let n_compare=1;

        /* Obtener primeros y ultimos N promedios */
        proc sql noprint;
            select Mean into :_tgt_means1 - :_tgt_means&num_months. from
                casuser._tgt_evolut_target;
        quit;

        /* Calcular promedios de primeros n_compare meses */
        %let first_mean=0;
        %do _i=1 %to &n_compare.;
            %let first_mean=%sysevalf(&first_mean. + &&_tgt_means&_i.);
        %end;
        %let first_mean=%sysevalf(&first_mean. / &n_compare.);

        /* Calcular promedios de ultimos n_compare meses */
        %let last_mean=0;
        %do _i=%eval(&num_months. - &n_compare. + 1) %to &num_months.;
            %let last_mean=%sysevalf(&last_mean. + &&_tgt_means&_i.);
        %end;
        %let last_mean=%sysevalf(&last_mean. / &n_compare.);

        /* Diferencia relativa */
        %if &first_mean. ne 0 %then %let relative_diff=%sysevalf((&last_mean. -
            &first_mean.) / &first_mean.);
        %else %let relative_diff=0;

        /* Tabla de resultados */
        data casuser._tgt_results;
            length Metric $ 40 Value 8.;
            %if &n_compare. >= 3 %then %do;
                Metric="Promedio de los primeros 3 meses";
            %end;
            %else %do;
                Metric="Primer mes";
            %end;
            Value=&first_mean.;
            output;

            %if &n_compare. >= 3 %then %do;
                Metric="Promedio de los ultimos 3 meses";
            %end;
            %else %do;
                Metric="Ultimo mes";
            %end;
            Value=&last_mean.;
            output;

            Metric="Diferencia relativa";
            Value=&relative_diff.;
            output;
        run;

        title "Resultados de la Diferencia Relativa";

        proc print data=casuser._tgt_results noobs;
            var Metric Value;
        run;
        title;

        proc datasets library=casuser nolist nowarn;
            delete _tgt_results;
        quit;
    %end;

    /* Grafico evolutivo */
    title "Evolutivo &target.";

    proc sgplot data=casuser._tgt_evolut_target;
        vline &byvar. / response=Mean markers markerattrs=(symbol=circlefilled
            color=black) lineattrs=(color=crimson);
        yaxis label="mean &target." min=0 max=1;
    run;
    title;

    /* Materialidad */
    title "Materialidad &data. Cerrado";

    proc freq data=casuser._tgt_data_filt;
        tables &byvar. * &target. / norow nopercent nocum nocol;
    run;
    title;

    /* Cleanup */
    proc datasets library=casuser nolist nowarn;
        delete _tgt_evolut_target _tgt_data_filt;
    quit;

%mend _target_describe;

/* =====================================================================
%_target_bandas - Bandas +/-2s sobre RD
Calcula mean/std desde TRAIN (is_train=1) y los guarda en macrovars
globales. En OOT (is_train=0) los reutiliza y luego los resetea.
===================================================================== */
%macro _target_bandas(data=, data_type=, target=, byvar=, is_train=1);

    %global _tgt_global_avg _tgt_std_monthly;
    %local inf sup min_val max_val;

    /* Calcular promedio mensual via FEDSQL */
    proc fedsql sessref=conn noprint;
        create table casuser._tgt_monthly {options replace=true} as select
            &byvar., avg(&target.) as avg_target from &data. group by &byvar.;
    quit;

    /* Calcular estadisticas globales si es TRAIN o no existen */
    %if (&is_train.=1) or (%length(&_tgt_global_avg.)=0) %then %do;
        proc sql noprint;
            select avg(&target.) into :_tgt_global_avg trimmed from &data.;
        quit;

        proc sql noprint;
            select std(avg_target) into :_tgt_std_monthly trimmed from
                casuser._tgt_monthly;
        quit;
    %end;
    %else %do;
        %put NOTE: [target_bandas] Usando estadisticas existentes
            (global_avg=&_tgt_global_avg. std=&_tgt_std_monthly.);
    %end;

    /* Guard: si std es vacio o missing (1 solo periodo), usar 0 */
    %if %length(&_tgt_std_monthly.)=0 or &_tgt_std_monthly.=. %then %let
        _tgt_std_monthly=0;

    /* Limites de bandas */
    %let inf=%sysevalf(&_tgt_global_avg. - 2 * &_tgt_std_monthly.);
    %let sup=%sysevalf(&_tgt_global_avg. + 2 * &_tgt_std_monthly.);
    %let min_val=%sysevalf(&_tgt_global_avg. - 3 * &_tgt_std_monthly.);
    %let max_val=%sysevalf(&_tgt_global_avg. + 3 * &_tgt_std_monthly.);

    /* Tabla con bandas para print */
    data casuser._tgt_monthly_bands;
        set casuser._tgt_monthly;
        lower_band=&inf.;
        upper_band=&sup.;
        global_avg=&_tgt_global_avg.;
        format avg_target lower_band upper_band global_avg 8.4;
    run;

    title "Evolutivo del Target - &data_type.";

    proc sgplot data=casuser._tgt_monthly subpixel noautolegend;
        band x=&byvar. lower=&inf. upper=&sup. / fillattrs=(color=graydd)
            legendlabel="+/- 2 Desv. Estandar" name="band1";
        series x=&byvar. y=avg_target / markers lineattrs=(color=blue
            thickness=2) legendlabel="RD" name="serie1";
        refline &_tgt_global_avg. / lineattrs=(color=red pattern=Dash)
            legendlabel="Overall Mean" name="line1";
        yaxis min=&min_val. max=&max_val. label="Promedio de &target.";
        xaxis label="&byvar." type=discrete;
        keylegend "serie1" "band1" / location=inside position=bottomright;
    run;
    title;

    proc print data=casuser._tgt_monthly_bands noobs;
        var &byvar. avg_target lower_band upper_band global_avg;
        label &byvar.="Periodo" avg_target="Promedio del Target"
            lower_band="Limite Inferior (- 2 Desv.)"
            upper_band="Limite Superior (+ 2 Desv.)"
            global_avg="Promedio Global";
    run;

    /* Resetear globales despues de OOT */
    %if &is_train.=0 %then %do;
        %let _tgt_global_avg=;
        %let _tgt_std_monthly=;
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _tgt_monthly _tgt_monthly_bands;
    quit;

%mend _target_bandas;

/* =====================================================================
%_target_ponderado_promedio - RD ponderado por monto (promedio)
sum(target*monto)/sum(monto) por periodo, con bandas +/-2s.
===================================================================== */
%macro _target_ponderado_promedio(data=, data_type=, target=, monto=, byvar=,
    is_train=1);

    %global _tgt_global_avg_pond _tgt_std_monthly_pond;
    %local inf sup min_val max_val _nobs_pond;

    /* Promedio ponderado por mes */
    proc fedsql sessref=conn noprint;
        create table casuser._tgt_monthly_pond {options replace=true} as select
            &byvar., sum(cast(&target. as double) * cast(&monto. as double)) /
            sum(cast(&monto. as double)) as avg_target_pond from &data. where
            &monto. > 0 group by &byvar.;
    quit;

    /* Guard: verificar que haya datos con monto > 0 */
    proc sql noprint;
        select count(*) into :_nobs_pond trimmed from casuser._tgt_monthly_pond;
    quit;

    %if &_nobs_pond.=0 %then %do;
        %put WARNING: [target_pond_prom] &data_type.: Sin datos con monto > 0.
            Se omite analisis ponderado promedio.;

        proc datasets library=casuser nolist nowarn;
            delete _tgt_monthly_pond;
        quit;
        /* Resetear globales si es OOT para no dejar basura */
        %if &is_train.=0 %then %do;
            %let _tgt_global_avg_pond=;
            %let _tgt_std_monthly_pond=;
        %end;
        %return;
    %end;

    /* Estadisticas globales si es TRAIN */
    %if (&is_train.=1) or (%length(&_tgt_global_avg_pond.)=0) %then %do;
        proc sql noprint;
            select sum(&target. * &monto.) / sum(&monto.) into
                :_tgt_global_avg_pond trimmed from &data. where &monto. > 0;
        quit;

        proc sql noprint;
            select std(avg_target_pond) into :_tgt_std_monthly_pond trimmed from
                casuser._tgt_monthly_pond;
        quit;
    %end;
    %else %do;
        %put NOTE: [target_pond_prom] Usando estadisticas existentes
            (avg_pond=&_tgt_global_avg_pond. std=&_tgt_std_monthly_pond.);
    %end;

    /* Guard: si avg_pond quedo vacio (imposible tras nobs check, pero seguro) */
    %if %length(&_tgt_global_avg_pond.)=0 or &_tgt_global_avg_pond.=. %then %do;
        %put WARNING: [target_pond_prom] &data_type.: Promedio ponderado no
            calculable. Se omite grafico.;

        proc datasets library=casuser nolist nowarn;
            delete _tgt_monthly_pond;
        quit;
        %if &is_train.=0 %then %do;
            %let _tgt_global_avg_pond=;
            %let _tgt_std_monthly_pond=;
        %end;
        %return;
    %end;

    /* Guard: si std es vacio o missing (1 solo periodo), usar 0 */
    %if %length(&_tgt_std_monthly_pond.)=0 or &_tgt_std_monthly_pond.=. %then
        %let _tgt_std_monthly_pond=0;

    %let inf=%sysevalf(&_tgt_global_avg_pond. - 2 * &_tgt_std_monthly_pond.);
    %let sup=%sysevalf(&_tgt_global_avg_pond. + 2 * &_tgt_std_monthly_pond.);
    %let min_val=%sysevalf(&_tgt_global_avg_pond. - 5 *
        &_tgt_std_monthly_pond.);
    %let max_val=%sysevalf(&_tgt_global_avg_pond. + 5 *
        &_tgt_std_monthly_pond.);

    data casuser._tgt_monthly_pond_b;
        set casuser._tgt_monthly_pond;
        lower_band=&inf.;
        upper_band=&sup.;
        global_mean=&_tgt_global_avg_pond.;
        format avg_target_pond lower_band upper_band global_mean 8.6;
    run;

    title "Target Ponderado por Monto - &data_type.";

    proc sgplot data=casuser._tgt_monthly_pond subpixel noautolegend;
        band x=&byvar. lower=&inf. upper=&sup. / fillattrs=(color=graydd)
            legendlabel="+/- 2 Desv. Estandar" name="band1";
        series x=&byvar. y=avg_target_pond / markers lineattrs=(color=darkblue
            thickness=2) legendlabel="RD Pond. Promedio" name="serie1";
        refline &_tgt_global_avg_pond. / lineattrs=(color=red pattern=Dash)
            legendlabel="Media Ponderada Global" name="line1";
        yaxis min=&min_val. max=&max_val. label="RD Pond. por Monto";
        xaxis label="&byvar." type=discrete;
        keylegend "serie1" "band1" "line1" / location=inside
            position=bottomright;
    run;
    title;

    proc print data=casuser._tgt_monthly_pond_b noobs;
        var &byvar. avg_target_pond lower_band upper_band global_mean;
        label &byvar.="Periodo" avg_target_pond="RD Ponderado por Monto"
            lower_band="Limite Inferior (- 2 Desv.)"
            upper_band="Limite Superior (+ 2 Desv.)"
            global_mean="Media Ponderada Global";
    run;

    /* Resetear globales despues de OOT */
    %if &is_train.=0 %then %do;
        %let _tgt_global_avg_pond=;
        %let _tgt_std_monthly_pond=;
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _tgt_monthly_pond _tgt_monthly_pond_b;
    quit;

%mend _target_ponderado_promedio;

/* =====================================================================
%_target_ponderado_suma - RD ponderado por monto (suma) + ratio
sum(target*monto) por periodo + ratio normalizado sobre monto total.
===================================================================== */
%macro _target_ponderado_suma(data=, data_type=, target=, monto=, byvar=,
    is_train=1);

    %global _tgt_global_sum_pond _tgt_std_sum_pond _tgt_global_ratio
        _tgt_std_ratio;
    %local inf sup inf_ratio sup_ratio min_ratio max_ratio _nobs_sum
        _all_monto_zero;

    /* Suma ponderada por mes */
    proc fedsql sessref=conn noprint;
        create table casuser._tgt_sum_pond {options replace=true} as select
            &byvar., sum(cast(&target. as double) * cast(&monto. as double)) as
            sum_target_pond, sum(cast(&monto. as double)) as total_monto from
            &data. group by &byvar.;
    quit;

    /* Guard: verificar que haya datos */
    proc sql noprint;
        select count(*) into :_nobs_sum trimmed from casuser._tgt_sum_pond;
    quit;

    %if &_nobs_sum.=0 %then %do;
        %put WARNING: [target_pond_sum] &data_type.: Tabla de sumas vacia. Se
            omite analisis ponderado suma.;

        proc datasets library=casuser nolist nowarn;
            delete _tgt_sum_pond;
        quit;
        %if &is_train.=0 %then %do;
            %let _tgt_global_sum_pond=;
            %let _tgt_std_sum_pond=;
            %let _tgt_global_ratio=;
            %let _tgt_std_ratio=;
        %end;
        %return;
    %end;

    /* Estadisticas globales si es TRAIN */
    %if (&is_train.=1) or (%length(&_tgt_global_sum_pond.)=0) %then %do;
        proc sql noprint;
            select mean(sum_target_pond) into :_tgt_global_sum_pond trimmed from
                casuser._tgt_sum_pond;
            select std(sum_target_pond) into :_tgt_std_sum_pond trimmed from
                casuser._tgt_sum_pond;
        quit;
    %end;
    %else %do;
        %put NOTE: [target_pond_sum] Usando estadisticas existentes
            (sum_pond=&_tgt_global_sum_pond. std=&_tgt_std_sum_pond.);
    %end;

    /* Guard: si global_sum_pond quedo vacio o missing */
    %if %length(&_tgt_global_sum_pond.)=0 or &_tgt_global_sum_pond.=. %then %do;
        %put WARNING: [target_pond_sum] &data_type.: Suma ponderada no
            calculable. Se omite grafico de sumas.;

        proc datasets library=casuser nolist nowarn;
            delete _tgt_sum_pond;
        quit;
        %if &is_train.=0 %then %do;
            %let _tgt_global_sum_pond=;
            %let _tgt_std_sum_pond=;
            %let _tgt_global_ratio=;
            %let _tgt_std_ratio=;
        %end;
        %return;
    %end;

    /* Guard: si std es vacio o missing (1 solo periodo), usar 0 */
    %if %length(&_tgt_std_sum_pond.)=0 or &_tgt_std_sum_pond.=. %then %let
        _tgt_std_sum_pond=0;

    %let inf=%sysevalf(&_tgt_global_sum_pond. - 2 * &_tgt_std_sum_pond.);
    %let sup=%sysevalf(&_tgt_global_sum_pond. + 2 * &_tgt_std_sum_pond.);

    data casuser._tgt_sum_pond_b;
        set casuser._tgt_sum_pond;
        lower_band=&inf.;
        upper_band=&sup.;
        global_mean=&_tgt_global_sum_pond.;
        format sum_target_pond lower_band upper_band global_mean total_monto
            comma18.2;
    run;

    title "Target Ponderado por Suma de Monto - &data_type.";

    proc sgplot data=casuser._tgt_sum_pond subpixel noautolegend;
        band x=&byvar. lower=&inf. upper=&sup. / fillattrs=(color=graydd)
            legendlabel="+/- 2 Desv. Estandar" name="band1";
        series x=&byvar. y=sum_target_pond / markers lineattrs=(color=darkgreen
            thickness=2) legendlabel="RD Pond. por Suma" name="serie1";
        refline &_tgt_global_sum_pond. / lineattrs=(color=red pattern=Dash)
            legendlabel="Media de Sumas Global" name="line1";
        yaxis label="RD Pond. por Suma de Monto";
        xaxis label="&byvar." type=discrete;
        keylegend "serie1" "band1" "line1" / location=inside
            position=bottomright;
    run;

    proc print data=casuser._tgt_sum_pond_b noobs;
        var &byvar. sum_target_pond total_monto lower_band upper_band
            global_mean;
        label &byvar.="Periodo" sum_target_pond="RD Ponderado por Suma"
            total_monto="Monto Total" lower_band="Limite Inferior (- 2 Desv.)"
            upper_band="Limite Superior (+ 2 Desv.)"
            global_mean="Media de Sumas Global";
    run;
    title;

    /* ---- Ratio normalizado: sum(target*monto)/sum(monto) por mes ------ */

    /* Guard: verificar si todos los total_monto son 0 (division por cero) */
    proc sql noprint;
        select (min(total_monto)=0 and max(total_monto)=0) into :_all_monto_zero
            trimmed from casuser._tgt_sum_pond;
    quit;

    %if &_all_monto_zero.=1 %then %do;
        %put WARNING: [target_pond_sum] &data_type.: Monto total=0 en todos los
            periodos. Se omite ratio RD/Monto.;
        /* Cleanup suma (ratio se omite) */
        %if &is_train.=0 %then %do;
            %let _tgt_global_sum_pond=;
            %let _tgt_std_sum_pond=;
            %let _tgt_global_ratio=;
            %let _tgt_std_ratio=;
        %end;

        proc datasets library=casuser nolist nowarn;
            delete _tgt_sum_pond _tgt_sum_pond_b;
        quit;
        %return;
    %end;

    /* Calcular ratio solo donde total_monto > 0 para evitar division por 0 */
    data casuser._tgt_ratio;
        set casuser._tgt_sum_pond;
        if total_monto > 0 then ratio_default_monto=sum_target_pond /
            total_monto;
        else ratio_default_monto=.;
    run;

    /* Estadisticas del ratio si es TRAIN */
    %if (&is_train.=1) or (%length(&_tgt_global_ratio.)=0) %then %do;
        proc sql noprint;
            select mean(ratio_default_monto) into :_tgt_global_ratio trimmed
                from casuser._tgt_ratio where ratio_default_monto is not
                missing;
            select std(ratio_default_monto) into :_tgt_std_ratio trimmed from
                casuser._tgt_ratio where ratio_default_monto is not missing;
        quit;
    %end;
    %else %do;
        %put NOTE: [target_pond_sum] Usando estadisticas ratio existentes
            (ratio=&_tgt_global_ratio. std=&_tgt_std_ratio.);
    %end;

    /* Guard: si ratio quedo vacio o missing */
    %if %length(&_tgt_global_ratio.)=0 or &_tgt_global_ratio.=. %then %do;
        %put WARNING: [target_pond_sum] &data_type.: Ratio no calculable. Se
            omite grafico de ratio.;
        %if &is_train.=0 %then %do;
            %let _tgt_global_sum_pond=;
            %let _tgt_std_sum_pond=;
            %let _tgt_global_ratio=;
            %let _tgt_std_ratio=;
        %end;

        proc datasets library=casuser nolist nowarn;
            delete _tgt_sum_pond _tgt_sum_pond_b _tgt_ratio;
        quit;
        %return;
    %end;

    /* Guard: si std_ratio es vacio o missing, usar 0 */
    %if %length(&_tgt_std_ratio.)=0 or &_tgt_std_ratio.=. %then %let
        _tgt_std_ratio=0;

    %let inf_ratio=%sysevalf(&_tgt_global_ratio. - 2 * &_tgt_std_ratio.);
    %let sup_ratio=%sysevalf(&_tgt_global_ratio. + 2 * &_tgt_std_ratio.);
    %let min_ratio=%sysevalf(&_tgt_global_ratio. - 5 * &_tgt_std_ratio.);
    %let max_ratio=%sysevalf(&_tgt_global_ratio. + 5 * &_tgt_std_ratio.);

    data casuser._tgt_ratio_b;
        set casuser._tgt_ratio;
        lower_band=&inf_ratio.;
        upper_band=&sup_ratio.;
        global_mean=&_tgt_global_ratio.;
        format ratio_default_monto lower_band upper_band global_mean 8.6;
    run;

    title "Ratio RD Ponderado sobre Monto Total - &data_type.";

    proc sgplot data=casuser._tgt_ratio subpixel noautolegend;
        band x=&byvar. lower=&inf_ratio. upper=&sup_ratio. /
            fillattrs=(color=graydd) legendlabel="+/- 2 Desv. Estandar"
            name="band1";
        series x=&byvar. y=ratio_default_monto / markers
            lineattrs=(color=darkred thickness=2) legendlabel="Ratio RD/Monto"
            name="serie1";
        refline &_tgt_global_ratio. / lineattrs=(color=blue pattern=Dash)
            legendlabel="Media del Ratio Global" name="line1";
        yaxis min=&min_ratio. max=&max_ratio. label="Ratio RD/Monto Total";
        xaxis label="&byvar." type=discrete;
        keylegend "serie1" "band1" "line1" / location=inside
            position=bottomright;
    run;
    title;

    proc print data=casuser._tgt_ratio_b noobs;
        var &byvar. ratio_default_monto lower_band upper_band global_mean;
        label &byvar.="Periodo" ratio_default_monto="Ratio RD/Monto"
            lower_band="Limite Inferior (- 2 Desv.)"
            upper_band="Limite Superior (+ 2 Desv.)"
            global_mean="Media del Ratio Global";
    run;

    /* Resetear globales despues de OOT */
    %if &is_train.=0 %then %do;
        %let _tgt_global_sum_pond=;
        %let _tgt_std_sum_pond=;
        %let _tgt_global_ratio=;
        %let _tgt_std_ratio=;
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _tgt_sum_pond _tgt_sum_pond_b _tgt_ratio _tgt_ratio_b;
    quit;

%mend _target_ponderado_suma;
