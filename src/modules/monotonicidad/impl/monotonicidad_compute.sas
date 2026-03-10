/* =========================================================================
monotonicidad_compute.sas - Core de calculo para Monotonicidad (METOD7)

Migra la logica legacy:
- Cortes por rank en TRAIN
- Reuso de cortes en OOT
- Bucketizacion de score
- Tabla por bucket: cuentas, pct_cuentas, mean(target)

Pattern B (work staging):
PROC RANK y joins iterativos se ejecutan en work.
========================================================================= */

/* =====================================================================
%_mono_calcular_cortes - calcula buckets/cortes para score numerico
Output:
- &out_cuts. con columnas:
  rango, inicio, fin, flag_ini, flag_fin, ETIQUETA
===================================================================== */
%macro _mono_calcular_cortes(tablain=, score_var=, groups=5,
    out_cuts=work._mono_cortes);

    %local _rnd _grp;
    %let _rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));
    %let _grp=&groups.;

    %if %length(%superq(_grp))=0 %then %let _grp=5;
    %if %sysevalf(&_grp. < 1) %then %let _grp=5;

    proc rank data=&tablain.(keep=&score_var. where=(not missing(&score_var.)))
        out=work._mono_rk_&_rnd.
        groups=&_grp.;
        var &score_var.;
        ranks rango_ini;
    run;

    proc sql noprint;
        create table work._mono_bins_&_rnd. as
        select rango_ini,
               min(&score_var.) as minval,
               max(&score_var.) as maxval
        from work._mono_rk_&_rnd.
        group by rango_ini
        order by rango_ini;
    quit;

    data work._mono_cuts_&_rnd.;
        set work._mono_bins_&_rnd. end=eof;
        length ETIQUETA $200;
        retain prev_fin .;

        rango = rango_ini + 1;
        inicio = prev_fin;
        fin = maxval;
        flag_ini = (_n_ = 1);
        flag_fin = eof;

        if flag_ini then inicio = .;

        if flag_ini then
            ETIQUETA = cats(put(rango, z2.), ". <-Inf; ", strip(put(fin, best12.4)), "]");
        else if flag_fin then
            ETIQUETA = cats(put(rango, z2.), ". <", strip(put(inicio, best12.4)), "; +Inf>");
        else
            ETIQUETA = cats(put(rango, z2.), ". <", strip(put(inicio, best12.4)),
                            "; ", strip(put(fin, best12.4)), "]");

        prev_fin = fin;
        keep rango inicio fin flag_ini flag_fin ETIQUETA;
    run;

    data work._mono_missing_&_rnd.;
        length ETIQUETA $200;
        rango = 0;
        inicio = .;
        fin = .;
        flag_ini = 0;
        flag_fin = 0;
        ETIQUETA = "00. Missing";
    run;

    data &out_cuts.;
        set work._mono_missing_&_rnd.
            work._mono_cuts_&_rnd.;
    run;

    proc sort data=&out_cuts.;
        by rango;
    run;

    proc datasets library=work nolist nowarn;
        delete _mono_rk_&_rnd.
               _mono_bins_&_rnd.
               _mono_cuts_&_rnd.
               _mono_missing_&_rnd.;
    quit;

%mend _mono_calcular_cortes;

/* =====================================================================
%_mono_build_report - genera tabla por bucket

Input:
- tablain          : dataset de analisis (work)
- score_var        : score/PD a granular
- target_var       : variable default
- groups           : numero de buckets rank
- use_existing_cuts: 0 calcula cortes, 1 reusa cortes previos
- cuts_table       : tabla de cortes (work)
- out_table        : tabla resumen por etiqueta
===================================================================== */
%macro _mono_build_report(tablain=, score_var=, target_var=, groups=5,
    use_existing_cuts=0, cuts_table=work._mono_cortes,
    out_table=work._mono_report);

    %local _mono_total _rnd;
    %let _rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    %if &use_existing_cuts.=0 %then %do;
        %_mono_calcular_cortes(tablain=&tablain., score_var=&score_var.,
            groups=&groups., out_cuts=&cuts_table.);
    %end;

    %let _mono_total=0;
    proc sql noprint;
        select count(*) into :_mono_total trimmed from &tablain.;
    quit;

    %if &_mono_total.=0 %then %do;
        data &out_table.;
            length ETIQUETA $200 Cuentas 8 Pct_cuentas 8 Mean_Default 8;
            stop;
        run;
        %return;
    %end;

    proc sql noprint;
        create table work._mono_tagged_&_rnd. as
        select a.*,
               coalesce(b.ETIQUETA, "00. Missing") as ETIQUETA length=200
        from &tablain. a
        left join &cuts_table. b
          on (missing(a.&score_var.) and b.rango = 0)
          or (
               not missing(a.&score_var.)
               and (
                    (b.flag_ini = 1 and a.&score_var. <= b.fin)
                 or (b.flag_fin = 1 and a.&score_var. > b.inicio)
                 or (b.flag_ini = 0 and b.flag_fin = 0
                     and a.&score_var. > b.inicio
                     and a.&score_var. <= b.fin)
               )
             );
    quit;

    proc sql noprint;
        create table &out_table. as
        select ETIQUETA,
               count(*) as Cuentas,
               count(*) / &_mono_total. as Pct_cuentas format=percent8.2,
               mean(&target_var.) as Mean_Default format=percent8.2
        from work._mono_tagged_&_rnd.
        group by ETIQUETA
        order by ETIQUETA;
    quit;

    proc datasets library=work nolist nowarn;
        delete _mono_tagged_&_rnd.;
    quit;

%mend _mono_build_report;

/* =====================================================================
%_mono_plot_and_print - grafico + tabla de salida por dataset
===================================================================== */
%macro _mono_plot_and_print(report_table=, score_var=, target_var=,
    data_type=);

    title "Granulado Score &score_var. - &data_type.";

    proc sgplot data=&report_table.;
        keylegend / title=" " opaque;
        vbar ETIQUETA / response=Pct_cuentas barwidth=0.4 nooutline;
        vline ETIQUETA / response=Mean_Default markers
            markerattrs=(symbol=circlefilled) y2axis;
        yaxis label="% Cuentas (bar)" discreteorder=data
            labelattrs=(size=8) valueattrs=(size=8);
        y2axis min=0 label="Mean &target_var." labelattrs=(size=8);
        xaxis label="Buckets &score_var." labelattrs=(size=8);
    run;
    title;

    proc print data=&report_table. noobs;
    run;

%mend _mono_plot_and_print;

