/* =========================================================================
similitud_compute.sas - Computo CAS-first de analisis de similitud

Principios:
- Input unificado con Split=TRAIN/OOT
- Intermedios en casuser (sin tablas operativas en work)
- Cortes numericos: calculados en TRAIN y reaplicados en OOT
- Sorting solo al final para lectura/visualizacion
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

%macro _simil_get_median(data=, split_var=Split, split_value=TRAIN, var=,
    outvar=_simil_median);

    %local _simil_n;
    %let &outvar=.;

    proc fedsql sessref=conn;
        create table casuser._simil_med_src {options replace=true} as
        select &var. as Valor
        from &data.
        where upcase(strip(&split_var.))='%upcase(&split_value.)'
          and &var. is not null;
    quit;

    proc fedsql sessref=conn;
        create table casuser._simil_med_ord {options replace=true} as
        select Valor,
               row_number() over(order by Valor) as RN,
               count(*) over() as N
        from casuser._simil_med_src;
    quit;

    %let _simil_n=0;
    proc sql noprint;
        select coalesce(max(N), 0) into :_simil_n trimmed
        from casuser._simil_med_ord;
    quit;

    %if &_simil_n. > 0 %then %do;
        %if %sysfunc(mod(&_simil_n., 2)) = 1 %then %do;
            proc sql noprint;
                select Valor into :&outvar. trimmed
                from casuser._simil_med_ord
                where RN = %sysfunc(ceil(%sysevalf((&_simil_n. + 1) / 2)));
            quit;
        %end;
        %else %do;
            proc sql noprint;
                select avg(Valor) into :&outvar. trimmed
                from casuser._simil_med_ord
                where RN in (%sysevalf(&_simil_n. / 2),
                    %sysevalf(&_simil_n. / 2 + 1));
            quit;
        %end;
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _simil_med_src _simil_med_ord;
    quit;

%mend _simil_get_median;

%macro _simil_get_mode_pct(data=, split_var=Split, split_value=TRAIN, var=,
    out_mode=_simil_mode, out_pct=_simil_pct);

    %local _simil_mode_n;
    %let &out_mode=;
    %let &out_pct=0;

    proc fedsql sessref=conn;
        create table casuser._simil_mode_freq {options replace=true} as
        select trim(cast(&var. as varchar(200))) as Category,
               count(*) as N
        from &data.
        where upcase(strip(&split_var.))='%upcase(&split_value.)'
          and &var. is not null
        group by trim(cast(&var. as varchar(200)));
    quit;

    proc fedsql sessref=conn;
        create table casuser._simil_mode_rank {options replace=true} as
        select Category,
               N,
               sum(N) over() as Tot,
               row_number() over(order by N desc, Category asc) as RN
        from casuser._simil_mode_freq;
    quit;

    %let _simil_mode_n=0;
    proc sql noprint;
        select count(*) into :_simil_mode_n trimmed
        from casuser._simil_mode_rank
        where RN=1;
    quit;

    %if &_simil_mode_n. > 0 %then %do;
        proc sql noprint;
            select Category,
                   case when Tot > 0 then 100 * N / Tot else 0 end
              into :&out_mode. trimmed,
                   :&out_pct. trimmed
            from casuser._simil_mode_rank
            where RN=1;
        quit;
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _simil_mode_freq _simil_mode_rank;
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

    proc fedsql sessref=conn;
        create table casuser._simil_num_rank_&rnd. {options replace=true} as
        select Valor,
               ntile(&groups.) over(order by Valor) as Rango
        from casuser._simil_num_src_&rnd.
        where Split='TRAIN'
          and Valor is not null;
    quit;

    %let _simil_rank_n=0;
    proc sql noprint;
        select count(*) into :_simil_rank_n trimmed
        from casuser._simil_num_rank_&rnd.;
    quit;

    %if &_simil_rank_n. > 0 %then %do;
        proc fedsql sessref=conn;
            create table casuser._simil_num_cuts_raw_&rnd.
                {options replace=true} as
            select Rango,
                   min(Valor) as MinVal,
                   max(Valor) as MaxVal
            from casuser._simil_num_rank_&rnd.
            group by Rango;
        quit;

        proc fedsql sessref=conn;
            create table casuser._simil_num_cuts_&rnd. {options replace=true} as
            select c.Rango,
                   p.MaxVal as Inicio,
                   c.MaxVal as Fin,
                   case when c.Rango = r.MinR then 1 else 0 end as Flag_Ini,
                   case when c.Rango = r.MaxR then 1 else 0 end as Flag_Fin
            from casuser._simil_num_cuts_raw_&rnd. c
            left join casuser._simil_num_cuts_raw_&rnd. p
                on c.Rango = p.Rango + 1
            cross join (
                select min(Rango) as MinR,
                       max(Rango) as MaxR
                from casuser._simil_num_cuts_raw_&rnd.
            ) r;
        quit;
    %end;
    %else %do;
        proc fedsql sessref=conn;
            create table casuser._simil_num_cuts_&rnd. {options replace=true} as
            select cast(. as double) as Rango,
                   cast(. as double) as Inicio,
                   cast(. as double) as Fin,
                   0 as Flag_Ini,
                   0 as Flag_Fin
            from casuser._simil_num_src_&rnd.
            where 1=0;
        quit;
    %end;

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
               _simil_num_rank_&rnd.
               _simil_num_cuts_raw_&rnd.
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
