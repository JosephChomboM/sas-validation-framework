/* =========================================================================
monotonicidad_compute.sas - Computo de monotonicidad (METOD7)

Contiene macros que discretizan variables numericas via PROC RANK,
reusan los cortes de TRAIN en OOT, y calculan distribucion + mean(target)
por bucket. Para categoricas, agrupa directamente.

Macros:
%_mono_calcular_cortes  - Calcula puntos de corte via PROC RANK
%_mono_tendencia        - Aplica cortes/agrupacion y genera tabla + grafico
%_mono_report_variables - Orquestador: itera vars num+cat por dataset

Pattern B:
- PROC RANK y la logica de bucketizacion corren en work.
- TRAIN calcula cortes; OOT los reutiliza.
========================================================================= */

/* =====================================================================
%_mono_calcular_cortes - calcula buckets/cortes para score numerico
Output:
- &out_cuts. con columnas:
  rango, inicio, fin, flag_ini, flag_fin, ETIQUETA
===================================================================== */
%macro _mono_calcular_cortes(tablain=, score_var=, groups=5,
    out_cuts=work.cortes);

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
%_mono_tendencia - Aplica cortes a datos y genera tabla + grafico
Para numericas (flg_continue=1): usa cortes de work.cortes.
Para categoricas (flg_continue=0): agrupa directamente.
===================================================================== */
%macro _mono_tendencia(tablain, var, target=, groups=5, flg_continue=1,
    reuse_cuts=0, m_data_type=);

    %local _mono_total _rnd;
    %let _rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    data work._mono_t_&rnd._0;
        set &tablain.;
        %if &flg_continue.=1 %then %do;
            if &var. in (., 1111111111, -1111111111, 2222222222, -2222222222,
                3333333333, -3333333333, 4444444444, 5555555555, 6666666666,
                7777777777, -999999999) then &var.=.;
        %end;
    run;

    %let _mono_total=0;
    proc sql noprint;
        select count(*) into :_mono_total trimmed from work._mono_t_&rnd._0;
    quit;

    %if &_mono_total.=0 %then %do;
        %return;
    %end;

    %if &flg_continue.=1 %then %do;
        %if &reuse_cuts.=0 %then %do;
            %_mono_calcular_cortes(tablain=work._mono_t_&rnd._0,
                score_var=&var., groups=&groups., out_cuts=work.cortes);
            proc sort data=work.cortes;
                by rango;
            run;
        %end;

        proc sql noprint;
            create table work._mono_tagged_&_rnd. as
            select a.*,
                   coalesce(b.ETIQUETA, "00. Missing") as ETIQUETA length=200
            from work._mono_t_&rnd._0 a
            left join work.cortes b
              on (missing(a.&var.) and b.rango = 0)
              or (
                   not missing(a.&var.)
                   and (
                        (b.flag_ini = 1 and a.&var. <= b.fin)
                     or (b.flag_fin = 1 and a.&var. > b.inicio)
                     or (b.flag_ini = 0 and b.flag_fin = 0
                         and a.&var. > b.inicio
                         and a.&var. <= b.fin)
                   )
                 );
        quit;

        proc sql noprint;
            create table work._mono_report as
            select ETIQUETA as &var.,
                   count(*) as Cuentas,
                   count(*) / &_mono_total. as Pct_cuentas format=percent8.2,
                   mean(&target.) as Mean_Default format=percent8.2
            from work._mono_tagged_&_rnd.
            group by ETIQUETA
            order by ETIQUETA;
        quit;
    %end;
    %else %do;
        proc sql noprint;
            create table work._mono_report as
            select &var.,
                   count(*) as Cuentas,
                   count(*) / &_mono_total. as Pct_cuentas format=percent8.2,
                   mean(&target.) as Mean_Default format=percent8.2
            from work._mono_t_&rnd._0
            group by &var.;
        quit;
    %end;

    title "Monotonicidad &var. - &m_data_type.";

    proc sgplot data=work._mono_report subpixel noautolegend;
        keylegend / title=" " opaque;
        vbar &var. / response=Pct_cuentas barwidth=0.4 nooutline;
        vline &var. / response=Mean_Default markers
            markerattrs=(symbol=circlefilled) y2axis;
        yaxis label="% Cuentas (bar)" discreteorder=data
            labelattrs=(size=8) valueattrs=(size=8);
        y2axis min=0 label="Mean &target." labelattrs=(size=8);
        xaxis label="Buckets variable" labelattrs=(size=8);
    run;
    title;

    proc print data=work._mono_report noobs;
    run;

    proc datasets library=work nolist nowarn;
        delete _mono_t_&rnd._0 _mono_tagged_&_rnd. _mono_report;
    quit;

%mend _mono_tendencia;

/* =====================================================================
%_mono_report_variables - Orquestador: itera vars num+cat por dataset
Para numericas:
- TRAIN: reuse_num_cuts=0 calcula cortes
- OOT:   reuse_num_cuts=1 reutiliza cortes previamente calculados
Para categoricas:
- agrupa directamente
===================================================================== */
%macro _mono_report_variables(data=, target=, vars_num=, vars_cat=, groups=5,
    data_type=, reuse_num_cuts=0, train_ref_data=);

    %local c v z v_cat;

    %if %length(&vars_num.) > 0 %then %do;
        %let c=1;
        %let v=%scan(&vars_num., &c., %str( ));
        %do %while(%length(&v.) > 0);
            %put NOTE: [monotonicidad] Procesando variable numerica: &v.;

            %if &reuse_num_cuts.=1 %then %do;
                %_mono_calcular_cortes(tablain=&train_ref_data.,
                    score_var=&v., groups=&groups., out_cuts=work.cortes);
                proc sort data=work.cortes;
                    by rango;
                run;
            %end;

            %_mono_tendencia(&data., &v., target=&target.,
                groups=&groups., flg_continue=1,
                reuse_cuts=&reuse_num_cuts., m_data_type=&data_type.);

            proc datasets library=work nolist nowarn;
                delete cortes;
            quit;

            %let c=%eval(&c. + 1);
            %let v=%scan(&vars_num., &c., %str( ));
        %end;
    %end;

    %if %length(&vars_cat.) > 0 %then %do;
        %let z=1;
        %let v_cat=%scan(&vars_cat., &z., %str( ));
        %do %while(%length(&v_cat.) > 0);
            %put NOTE: [monotonicidad] Procesando variable categorica: &v_cat.;

            %_mono_tendencia(&data., &v_cat., target=&target.,
                groups=&groups., flg_continue=0, reuse_cuts=0,
                m_data_type=&data_type.);

            %let z=%eval(&z. + 1);
            %let v_cat=%scan(&vars_cat., &z., %str( ));
        %end;
    %end;

%mend _mono_report_variables;
