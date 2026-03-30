/* =========================================================================
similitud_compute.sas - Computo CAS-first de analisis de similitud

Principios:
- Input unificado con Split=TRAIN/OOT
- Intermedios en casuser (sin tablas operativas en work)
- Cortes numericos: calculados en TRAIN y reaplicados en OOT
- Sorting solo al final para lectura/visualizacion

Nota:
- Para operaciones no soportadas de forma estable en FedSQL de este entorno
  (ranking, mediana y moda), se usa staging minimo en work y luego se vuelve
  a CAS para el resto del flujo.
========================================================================= */

%macro _simil_sort_cas(table_name=, orderby=, groupby={});

    %if %length(%superq(table_name))=0 or %length(%superq(orderby))=0 %then
        %return;

    proc cas;
        session conn;
        table.partition /
            table={
                caslib="casuser",
                name="&table_name.",
                orderby=&orderby.,
                groupby=&groupby.
            },
            casout={
                caslib="casuser",
                name="&table_name.",
                replace=true
            };
    quit;

%mend _simil_sort_cas;

%macro _simil_build_num_cuts(data=, split_var=Split, split_value=TRAIN, var=,
    groups=5, out_table=_simil_num_cuts);

    %local rnd _simil_nobs;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));
    %let _simil_nobs=0;

    data work._simil_rk_&rnd._1;
        set &data.(keep=&split_var. &var.
            where=(upcase(strip(&split_var.))="%upcase(&split_value.)"));
        if &var. in (., 1111111111, -1111111111, 2222222222, -2222222222,
            3333333333, -3333333333, 4444444444, 5555555555, 6666666666,
            7777777777, -999999999) then &var.=.;
        &var.=round(&var., 0.0001);
        keep &var.;
    run;

    proc sql noprint;
        select count(*) into :_simil_nobs trimmed
        from work._simil_rk_&rnd._1;
    quit;

    %if &_simil_nobs. > 0 %then %do;
        proc rank data=work._simil_rk_&rnd._1 out=work._simil_rk_&rnd._2
            groups=&groups.;
            ranks RANGO;
            var &var.;
        run;

        proc sql noprint;
            create table work._simil_rk_&rnd._3 as
            select RANGO,
                   min(&var.) as MINVAL,
                   max(&var.) as MAXVAL
            from work._simil_rk_&rnd._2
            group by RANGO;
        quit;

        proc sort data=work._simil_rk_&rnd._3;
            by RANGO;
        run;

        data work._simil_rk_&rnd._4;
            set work._simil_rk_&rnd._3(rename=(RANGO=RANGO_INI)) end=EOF;
            retain MARCA 0;
            FLAG_INI=0;
            FLAG_FIN=0;
            LAGMAXVAL=lag(MAXVAL);
            RANGO=RANGO_INI + 1;
            if RANGO_INI=. then RANGO=0;
            if RANGO_INI >= 0 then MARCA + 1;
            if MARCA=1 then FLAG_INI=1;
            if EOF then FLAG_FIN=1;
        run;

        proc sql noprint;
            create table work._simil_cuts_&rnd. as
            select "&var." as VARIABLE length=32,
                   RANGO,
                   RANGO_INI,
                   LAGMAXVAL as INICIO,
                   MAXVAL as FIN,
                   FLAG_INI,
                   FLAG_FIN,
                   case
                       when RANGO = 0 then "00. Missing"
                       when FLAG_INI = 1 then
                           cat(put(RANGO, Z2.), ". <-Inf; ",
                               cats(put(MAXVAL, F12.4)), "]")
                       when FLAG_FIN = 1 then
                           cat(put(RANGO, Z2.), ". <",
                               cats(put(LAGMAXVAL, F12.4)), "; +Inf>")
                       else
                           cat(put(RANGO, Z2.), ". <",
                               cats(put(LAGMAXVAL, F12.4)), "; ",
                               cats(put(MAXVAL, F12.4)), "]")
                   end as ETIQUETA length=200
            from work._simil_rk_&rnd._4;
        quit;
    %end;
    %else %do;
        data work._simil_cuts_&rnd.;
            length VARIABLE $32 RANGO 8 RANGO_INI 8 INICIO 8 FIN 8
                FLAG_INI 8 FLAG_FIN 8 ETIQUETA $200;
            stop;
        run;
    %end;

    data casuser.&out_table.;
        set work._simil_cuts_&rnd.;
    run;

    proc datasets library=work nolist nowarn;
        delete _simil_rk_&rnd.: _simil_cuts_&rnd.;
    quit;

%mend _simil_build_num_cuts;

%macro _simil_get_median(data=, split_var=Split, split_value=TRAIN, var=,
    outvar=_simil_median);

    %local _simil_n _rank_lo _rank_hi _simil_med_lo _simil_med_hi;
    %let &outvar=.;

    data work._simil_med_src;
        set &data.(keep=&split_var. &var.
            where=(upcase(strip(&split_var.))="%upcase(&split_value.)"
                and not missing(&var.)));
        Valor=&var.;
        keep Valor;
    run;

    %let _simil_n=0;
    proc sql noprint;
        select count(*) into :_simil_n trimmed
        from work._simil_med_src;
    quit;

    %if &_simil_n. > 0 %then %do;
        proc sort data=work._simil_med_src;
            by Valor;
        run;

        %let _rank_lo=%sysfunc(floor(%sysevalf((&_simil_n. + 1) / 2)));
        %let _rank_hi=%sysfunc(ceil(%sysevalf((&_simil_n. + 1) / 2)));
        %let _simil_med_lo=.;
        %let _simil_med_hi=.;

        data _null_;
            set work._simil_med_src;
            if _n_=&_rank_lo. then call symputx('_simil_med_lo', Valor);
            if _n_=&_rank_hi. then call symputx('_simil_med_hi', Valor);
        run;

        %if %sysevalf(%superq(_simil_med_lo)=, boolean) %then
            %let _simil_med_lo=.;
        %if %sysevalf(%superq(_simil_med_hi)=, boolean) %then
            %let _simil_med_hi=.;

        %if %sysfunc(mod(&_simil_n., 2)) = 1 %then
            %let &outvar=&_simil_med_hi.;
        %else
            %let &outvar=%sysevalf((&_simil_med_lo. + &_simil_med_hi.) / 2);
    %end;

    proc datasets library=work nolist nowarn;
        delete _simil_med_src;
    quit;

%mend _simil_get_median;

%macro _simil_get_mode_pct(data=, split_var=Split, split_value=TRAIN, var=,
    out_mode=_simil_mode, out_pct=_simil_pct);

    %local _simil_mode_n;
    %let &out_mode=;
    %let &out_pct=0;

    data work._simil_mode_src;
        set &data.(keep=&split_var. &var.
            where=(upcase(strip(&split_var.))="%upcase(&split_value.)"));
        if missing(&var.) then delete;
    run;

    proc freq data=work._simil_mode_src noprint;
        tables &var. / out=work._simil_mode_freq;
    run;

    proc sort data=work._simil_mode_freq;
        by descending count &var.;
    run;

    %let _simil_mode_n=0;
    proc sql noprint;
        select count(*) into :_simil_mode_n trimmed
        from work._simil_mode_freq;
    quit;

    %if &_simil_mode_n. > 0 %then %do;
        data _null_;
            set work._simil_mode_freq(obs=1);
            call symputx("&out_mode.", strip(vvaluex("&var.")));
            call symputx("&out_pct.", percent);
        run;
    %end;

    proc datasets library=work nolist nowarn;
        delete _simil_mode_src _simil_mode_freq;
    quit;

%mend _simil_get_mode_pct;

%macro _simil_bucket_plot_num(data=, split_var=Split, var=, byvar=,
    groups=5, m_data_type=TRAIN);

    %local rnd _simil_rank_n;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    proc fedsql sessref=conn;
        create table casuser._simil_num_src_&rnd. {options replace=true} as
        select cast(upcase(strip(&split_var.)) as varchar(5)) as Split,
               &byvar. as Periodo,
               case
                   when &var. in (., 1111111111, -1111111111, 2222222222,
                       -2222222222, 3333333333, -3333333333, 4444444444,
                       5555555555, 6666666666, 7777777777, -999999999)
                   then .
                   else round(&var., 0.0001)
               end as Valor
        from &data.
        where upcase(strip(&split_var.)) in ('TRAIN', 'OOT');
    quit;

    %_simil_build_num_cuts(data=casuser._simil_num_src_&rnd., split_var=Split,
        split_value=TRAIN, var=Valor, groups=&groups.,
        out_table=_simil_num_cuts_&rnd.);

    proc fedsql sessref=conn;
        create table casuser._simil_num_bucket_&rnd. {options replace=true} as
        select a.Periodo,
               case
                   when a.Valor is null then 0
                   when c.Rango is null then 999
                   else c.Rango
               end as Bucket_N,
               case
                   when a.Valor is null then '00. Missing'
                   when c.Rango is null then '99. OutRange'
                   when c.Rango < 10 then '0' || trim(cast(c.Rango as varchar(16)))
                   else trim(cast(c.Rango as varchar(16)))
               end as Bucket
        from casuser._simil_num_src_&rnd. a
        left join casuser._simil_num_cuts_&rnd. c
            on upcase(strip(a.Split))='%upcase(&m_data_type.)'
           and (
               (c.Flag_Ini=1 and c.Flag_Fin=1 and a.Valor is not null)
               or
               (c.Flag_Ini=1 and c.Flag_Fin=0 and a.Valor <= c.Fin)
               or
               (c.Flag_Ini=0 and c.Flag_Fin=1 and a.Valor > c.Inicio)
               or
               (c.Flag_Ini=0 and c.Flag_Fin=0 and a.Valor > c.Inicio and a.Valor <= c.Fin)
           )
        where upcase(strip(a.Split))='%upcase(&m_data_type.)';
    quit;

    proc fedsql sessref=conn;
        create table casuser._simil_num_cnt_&rnd. {options replace=true} as
        select Periodo,
               Bucket_N,
               Bucket,
               count(*) as N
        from casuser._simil_num_bucket_&rnd.
        group by Periodo, Bucket_N, Bucket;
    quit;

    proc fedsql sessref=conn;
        create table casuser._simil_num_tot_&rnd. {options replace=true} as
        select Periodo,
               sum(N) as Total_N
        from casuser._simil_num_cnt_&rnd.
        group by Periodo;
    quit;

    proc fedsql sessref=conn;
        create table casuser._simil_num_pct_&rnd. {options replace=true} as
        select c.Periodo,
               c.Bucket_N,
               c.Bucket,
               c.N,
               case when t.Total_N > 0 then 100 * c.N / t.Total_N else 0 end as Percent
        from casuser._simil_num_cnt_&rnd. c
        inner join casuser._simil_num_tot_&rnd. t
            on c.Periodo=t.Periodo;
    quit;

    %_simil_sort_cas(table_name=_simil_num_pct_&rnd.,
        orderby=%str({"Periodo", "Bucket_N"}));

    title "Evolutivo distribucion variable &var. - %upcase(&m_data_type.).";
    proc sgplot data=casuser._simil_num_pct_&rnd.;
        vbar Periodo / response=Percent group=Bucket
            groupdisplay=stack nooutline name='bars' barwidth=1;
        keylegend 'bars' / title='Rango' opaque;
        xaxis type=discrete discreteorder=data valueattrs=(size=7pt);
        yaxis label='Percent';
    run;
    title;

    ods escapechar='^';
    ods text=' ';
    ods text="^S={fontweight=bold fontsize=11pt} Bucket % de &var. por &byvar. - %upcase(&m_data_type.).";
    ods text=' ';

    proc print data=casuser._simil_num_pct_&rnd. noobs;
        var Periodo Bucket N Percent;
        format Percent 8.2;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _simil_num_src_&rnd.
               _simil_num_cuts_&rnd.
               _simil_num_bucket_&rnd.
               _simil_num_cnt_&rnd.
               _simil_num_tot_&rnd.
               _simil_num_pct_&rnd.;
    quit;

%mend _simil_bucket_plot_num;

%macro _simil_bucket_plot_cat(data=, split_var=Split, var=, byvar=,
    m_data_type=TRAIN);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    proc fedsql sessref=conn;
        create table casuser._simil_cat_src_&rnd. {options replace=true} as
        select &byvar. as Periodo,
               coalesce(trim(cast(&var. as varchar(200))), '00. Missing') as Bucket
        from &data.
        where upcase(strip(&split_var.))='%upcase(&m_data_type.)';
    quit;

    proc fedsql sessref=conn;
        create table casuser._simil_cat_cnt_&rnd. {options replace=true} as
        select Periodo,
               Bucket,
               count(*) as N
        from casuser._simil_cat_src_&rnd.
        group by Periodo, Bucket;
    quit;

    proc fedsql sessref=conn;
        create table casuser._simil_cat_tot_&rnd. {options replace=true} as
        select Periodo,
               sum(N) as Total_N
        from casuser._simil_cat_cnt_&rnd.
        group by Periodo;
    quit;

    proc fedsql sessref=conn;
        create table casuser._simil_cat_pct_&rnd. {options replace=true} as
        select c.Periodo,
               c.Bucket,
               c.N,
               case when t.Total_N > 0 then 100 * c.N / t.Total_N else 0 end as Percent
        from casuser._simil_cat_cnt_&rnd. c
        inner join casuser._simil_cat_tot_&rnd. t
            on c.Periodo=t.Periodo;
    quit;

    %_simil_sort_cas(table_name=_simil_cat_pct_&rnd.,
        orderby=%str({"Periodo", "Bucket"}));

    title "Evolutivo distribucion variable &var. - %upcase(&m_data_type.).";
    proc sgplot data=casuser._simil_cat_pct_&rnd.;
        vbar Periodo / response=Percent group=Bucket
            groupdisplay=stack nooutline name='bars' barwidth=1;
        keylegend 'bars' / title='Categoria' opaque;
        xaxis type=discrete discreteorder=data valueattrs=(size=7pt);
        yaxis label='Percent';
    run;
    title;

    ods escapechar='^';
    ods text=' ';
    ods text="^S={fontweight=bold fontsize=11pt} Bucket % de &var. por &byvar. - %upcase(&m_data_type.).";
    ods text=' ';

    proc print data=casuser._simil_cat_pct_&rnd. noobs;
        var Periodo Bucket N Percent;
        format Percent 8.2;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _simil_cat_src_&rnd.
               _simil_cat_cnt_&rnd.
               _simil_cat_tot_&rnd.
               _simil_cat_pct_&rnd.;
    quit;

%mend _simil_bucket_plot_cat;

%macro _simil_bucket_variables(data=, split_var=Split, byvar=, vars_num=,
    vars_cat=, groups=5);

    %local c v z v_cat;

    %if %length(%superq(vars_num)) > 0 %then %do;
        %let c=1;
        %let v=%scan(&vars_num., &c., %str( ));
        %do %while(%length(&v.) > 0);
            %put NOTE: [similitud] Bucket plot numerica: &v.;
            %_simil_bucket_plot_num(data=&data., split_var=&split_var.,
                var=&v., byvar=&byvar., groups=&groups., m_data_type=TRAIN);
            %_simil_bucket_plot_num(data=&data., split_var=&split_var.,
                var=&v., byvar=&byvar., groups=&groups., m_data_type=OOT);
            %let c=%eval(&c. + 1);
            %let v=%scan(&vars_num., &c., %str( ));
        %end;
    %end;

    %if %length(%superq(vars_cat)) > 0 %then %do;
        %let z=1;
        %let v_cat=%scan(&vars_cat., &z., %str( ));
        %do %while(%length(&v_cat.) > 0);
            %put NOTE: [similitud] Bucket plot categorica: &v_cat.;
            %_simil_bucket_plot_cat(data=&data., split_var=&split_var.,
                var=&v_cat., byvar=&byvar., m_data_type=TRAIN);
            %_simil_bucket_plot_cat(data=&data., split_var=&split_var.,
                var=&v_cat., byvar=&byvar., m_data_type=OOT);
            %let z=%eval(&z. + 1);
            %let v_cat=%scan(&vars_cat., &z., %str( ));
        %end;
    %end;

%mend _simil_bucket_variables;

%macro _simil_similitud_num(data=, split_var=Split, vars_num=, target=,
    umbral_verde=10, umbral_amarillo=20);

    %local todas_vars total_vars i var_num;
    %local mediana_train mediana_oot mae rmse diferencia_pct similitud;

    %let todas_vars=&target. &vars_num.;

    %if %length(%superq(todas_vars)) = 0 %then %do;
        %put WARNING: [similitud] No hay variables numericas para similitud.;
        %return;
    %end;

    data casuser._simil_res_num;
        length Variable $64 Mediana_TRAIN 8 Mediana_OOT 8 MAE 8 RMSE 8
            Diferencia_Pct 8 Similitud $20;
        stop;
    run;

    %let total_vars=%sysfunc(countw(&todas_vars., %str( )));

    %do i=1 %to &total_vars.;
        %let var_num=%scan(&todas_vars., &i., %str( ));
        %put NOTE: [similitud] Similitud numerica: &var_num.;

        %_simil_get_median(data=&data., split_var=&split_var.,
            split_value=TRAIN, var=&var_num., outvar=mediana_train);
        %_simil_get_median(data=&data., split_var=&split_var.,
            split_value=OOT, var=&var_num., outvar=mediana_oot);

        %if %sysevalf(%superq(mediana_train)=, boolean) %then %let mediana_train=.;
        %if %sysevalf(%superq(mediana_oot)=, boolean) %then %let mediana_oot=.;

        %let mae=%sysfunc(abs(%sysevalf(&mediana_train. - &mediana_oot.)));
        %let rmse=%sysfunc(sqrt((&mediana_train. - &mediana_oot.)**2));

        %if %sysevalf(&mediana_train. ^= 0) %then %do;
            %let diferencia_pct=%sysevalf(100 * &mae. /
                %sysfunc(abs(&mediana_train.)));
        %end;
        %else %if %sysevalf(&mediana_oot. = 0) %then %do;
            %let diferencia_pct=0;
        %end;
        %else %do;
            %let diferencia_pct=100;
        %end;

        %if %sysevalf(&diferencia_pct. < &umbral_verde.) %then
            %let similitud=Alta Similitud;
        %else %if %sysevalf(&diferencia_pct. < &umbral_amarillo.) %then
            %let similitud=Similitud Media;
        %else
            %let similitud=Baja Similitud;

        data casuser._simil_tmp_num;
            length Variable $64 Mediana_TRAIN 8 Mediana_OOT 8 MAE 8 RMSE 8
                Diferencia_Pct 8 Similitud $20;
            Variable="&var_num.";
            Mediana_TRAIN=&mediana_train.;
            Mediana_OOT=&mediana_oot.;
            MAE=&mae.;
            RMSE=&rmse.;
            Diferencia_Pct=&diferencia_pct.;
            Similitud="&similitud.";
            output;
        run;

        proc cas;
            session conn;
            table.append /
                source={caslib='casuser', name='_simil_tmp_num'},
                target={caslib='casuser', name='_simil_res_num'};
        quit;

        proc datasets library=casuser nolist nowarn;
            delete _simil_tmp_num;
        quit;
    %end;

    %_simil_sort_cas(table_name=_simil_res_num,
        orderby=%str({"Variable"}));

    proc format;
        value $simil_bg
            'Alta Similitud'='lightgreen'
            'Similitud Media'='yellow'
            'Baja Similitud'='salmon';
    run;

    title "Similitud de muestras - Variables Numericas (Mediana)";
    proc print data=casuser._simil_res_num label noobs;
        var Variable Mediana_TRAIN Mediana_OOT MAE RMSE Diferencia_Pct;
        var Similitud / style={background=$simil_bg.};
        format Mediana_TRAIN Mediana_OOT 12.4 MAE RMSE 12.4
            Diferencia_Pct 8.1;
        label MAE='Error Abs. Medio'
              RMSE='Raiz Error Cuad.'
              Diferencia_Pct='Diferencia (%)'
              Similitud='Nivel de Similitud';
    run;
    title;

    proc datasets library=casuser nolist nowarn;
        delete _simil_res_num;
    quit;

%mend _simil_similitud_num;

%macro _simil_similitud_cat(data=, split_var=Split, vars_cat=,
    umbral_verde=10, umbral_amarillo=20);

    %local total_vars i var_cat;
    %local moda_train moda_oot pct_train pct_oot diferencia similitud;

    %if %length(%superq(vars_cat)) = 0 %then %do;
        %put WARNING: [similitud] No hay variables categoricas para similitud.;
        %return;
    %end;

    data casuser._simil_res_cat;
        length Variable $64 Moda_TRAIN $200 Moda_OOT $200 Pct_TRAIN 8
            Pct_OOT 8 Diferencia 8 Similitud $20;
        stop;
    run;

    %let total_vars=%sysfunc(countw(&vars_cat., %str( )));

    %do i=1 %to &total_vars.;
        %let var_cat=%scan(&vars_cat., &i., %str( ));
        %put NOTE: [similitud] Similitud categorica: &var_cat.;

        %_simil_get_mode_pct(data=&data., split_var=&split_var.,
            split_value=TRAIN, var=&var_cat., out_mode=moda_train,
            out_pct=pct_train);
        %_simil_get_mode_pct(data=&data., split_var=&split_var.,
            split_value=OOT, var=&var_cat., out_mode=moda_oot,
            out_pct=pct_oot);

        %if %sysevalf(%superq(pct_train)=, boolean) %then %let pct_train=0;
        %if %sysevalf(%superq(pct_oot)=, boolean) %then %let pct_oot=0;

        %let diferencia=%sysfunc(abs(%sysevalf(&pct_train. - &pct_oot.)));

        %if %sysevalf(&diferencia. < &umbral_verde.) %then
            %let similitud=Alta Similitud;
        %else %if %sysevalf(&diferencia. < &umbral_amarillo.) %then
            %let similitud=Similitud Media;
        %else
            %let similitud=Baja Similitud;

        data casuser._simil_tmp_cat;
            length Variable $64 Moda_TRAIN $200 Moda_OOT $200 Pct_TRAIN 8
                Pct_OOT 8 Diferencia 8 Similitud $20;
            Variable="&var_cat.";
            Moda_TRAIN=symget('moda_train');
            Moda_OOT=symget('moda_oot');
            Pct_TRAIN=&pct_train.;
            Pct_OOT=&pct_oot.;
            Diferencia=&diferencia.;
            Similitud="&similitud.";
            output;
        run;

        proc cas;
            session conn;
            table.append /
                source={caslib='casuser', name='_simil_tmp_cat'},
                target={caslib='casuser', name='_simil_res_cat'};
        quit;

        proc datasets library=casuser nolist nowarn;
            delete _simil_tmp_cat;
        quit;
    %end;

    %_simil_sort_cas(table_name=_simil_res_cat,
        orderby=%str({"Variable"}));

    proc format;
        value $simil_bg
            'Alta Similitud'='lightgreen'
            'Similitud Media'='yellow'
            'Baja Similitud'='salmon';
    run;

    title "Similitud de muestras - Variables Categoricas (Moda)";
    proc print data=casuser._simil_res_cat label noobs;
        var Variable Moda_TRAIN Pct_TRAIN Moda_OOT Pct_OOT Diferencia;
        var Similitud / style={background=$simil_bg.};
        format Pct_TRAIN Pct_OOT 8.1 Diferencia 8.1;
        label Diferencia='Diferencia (%)'
              Similitud='Nivel de Similitud';
    run;
    title;

    proc datasets library=casuser nolist nowarn;
        delete _simil_res_cat;
    quit;

%mend _simil_similitud_cat;
