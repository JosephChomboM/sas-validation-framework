/* =========================================================================
target_compute.sas - Computo CAS-first para Target (METOD2.1)
========================================================================= */

%macro _target_prepare_inputs(input_caslib=, train_table=, oot_table=,
    byvar=, target=, monto_var=, def_cld=, has_monto=0);

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="_tgt_train" quiet=true;
        table.dropTable / caslib="casuser" name="_tgt_oot" quiet=true;
        table.dropTable / caslib="casuser" name="_tgt_all" quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser._tgt_train {options replace=true} as
        select 'TRAIN' as Split,
            &byvar. as Periodo,
            &target. as Target
            %if &has_monto.=1 %then %do;
                , &monto_var. as Monto
            %end;
        from &input_caslib..&train_table.
        where &byvar. <= &def_cld.;

        create table casuser._tgt_oot {options replace=true} as
        select 'OOT' as Split,
            &byvar. as Periodo,
            &target. as Target
            %if &has_monto.=1 %then %do;
                , &monto_var. as Monto
            %end;
        from &input_caslib..&oot_table.
        where &byvar. <= &def_cld.;

        create table casuser._tgt_all {options replace=true} as
        select * from casuser._tgt_train;
    quit;

    proc cas;
        session conn;
        table.append /
            source={caslib="casuser", name="_tgt_oot"},
            target={caslib="casuser", name="_tgt_all"};
        table.dropTable / caslib="casuser" name="_tgt_train" quiet=true;
        table.dropTable / caslib="casuser" name="_tgt_oot" quiet=true;
    quit;

%mend _target_prepare_inputs;

%macro _target_compute_monthly_rd(out=casuser._tgt_rd_monthly);

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select Split,
            Periodo,
            count(*) as N_Total,
            count(Target) as N_Valid,
            sum(case when Target=1 then 1 else 0 end) as N_Default,
            avg(Target) as RD
        from casuser._tgt_all
        group by Split, Periodo
        order by Split, Periodo;
    quit;

%mend _target_compute_monthly_rd;

%macro _target_sort_cas(table_name=, orderby=);

    %if %length(%superq(table_name))=0 or %length(%superq(orderby))=0 %then
        %return;

    proc cas;
        session conn;
        table.partition /
            table={
                caslib="casuser",
                name="&table_name.",
                orderby=&orderby.,
                groupby={}
            },
            casout={
                caslib="casuser",
                name="&table_name.",
                replace=true
            };
    quit;

%mend _target_sort_cas;

%macro _target_init_rel_diff(out=casuser._tgt_rel_diff);

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="_tgt_rel_diff" quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select cast('' as varchar(5)) as Split,
            cast(0 as double) as N_Months,
            cast(0 as double) as Window_Size,
            cast('' as varchar(12)) as Window_Type,
            cast('' as varchar(40)) as Start_Label,
            cast(0 as double) as Start_Value,
            cast('' as varchar(40)) as End_Label,
            cast(0 as double) as End_Value,
            cast(0 as double) as Relative_Diff,
            cast('' as varchar(120)) as Note
        from casuser._tgt_rd_monthly
        where 1=0;
    quit;

%mend _target_init_rel_diff;

%macro _target_rel_diff_split(split=, monthly=casuser._tgt_rd_monthly,
    out=casuser._tgt_rel_diff);

    %local _n_months _take _start_label _end_label _window_type _first_mean
        _last_mean _note _j;

    %let _n_months=0;
    %let _take=0;
    %let _start_label=;
    %let _end_label=;
    %let _window_type=;
    %let _first_mean=.;
    %let _last_mean=.;
    %let _note=;

    proc sql noprint;
        select count(*) into :_n_months trimmed
        from &monthly.
        where Split="&split.";
    quit;

    %if &_n_months.=0 %then %return;

    %if &_n_months. >= 6 %then %do;
        %let _take=3;
        %let _start_label=Promedio primeros 3 meses;
        %let _end_label=Promedio ultimos 3 meses;
        %let _window_type=3V3;
    %end;
    %else %if &_n_months. > 1 %then %do;
        %let _take=1;
        %let _start_label=Primer mes;
        %let _end_label=Ultimo mes;
        %let _window_type=1V1;
    %end;
    %else %do;
        proc fedsql sessref=conn;
            create table casuser._tgt_rel_row {options replace=true} as
            select min(Split) as Split,
                &_n_months. as N_Months,
                1 as Window_Size,
                'SIN_COMP' as Window_Type,
                'Primer mes' as Start_Label,
                . as Start_Value,
                'Ultimo mes' as End_Label,
                . as End_Value,
                . as Relative_Diff,
                'Solo un periodo; no aplica diferencia relativa.' as Note
            from &monthly.
            where Split="&split.";
        quit;

        proc cas;
            session conn;
            table.append /
                source={caslib="casuser", name="_tgt_rel_row"},
                target={caslib="casuser", name="_tgt_rel_diff"};
            table.dropTable / caslib="casuser" name="_tgt_rel_row" quiet=true;
        quit;
        %return;
    %end;

    %let _first_mean=0;
    proc sql noprint outobs=&_take.;
        select RD into :_tgt_first1-:_tgt_first&_take.
        from &monthly.
        where Split="&split."
        order by Periodo;
    quit;

    %do _j=1 %to &_take.;
        %if %length(%superq(_tgt_first&_j.)) > 0 %then
            %let _first_mean=%sysevalf(&_first_mean. + &&_tgt_first&_j..);
    %end;
    %let _first_mean=%sysevalf(&_first_mean. / &_take.);

    %let _last_mean=0;
    proc sql noprint outobs=&_take.;
        select RD into :_tgt_last1-:_tgt_last&_take.
        from &monthly.
        where Split="&split."
        order by Periodo desc;
    quit;

    %do _j=1 %to &_take.;
        %if %length(%superq(_tgt_last&_j.)) > 0 %then
            %let _last_mean=%sysevalf(&_last_mean. + &&_tgt_last&_j..);
    %end;
    %let _last_mean=%sysevalf(&_last_mean. / &_take.);

    %if %sysevalf(%superq(_first_mean)=, boolean) %then %let _first_mean=.;
    %if %sysevalf(%superq(_last_mean)=, boolean) %then %let _last_mean=.;

    %if %sysevalf(%superq(_first_mean)=., boolean) or
        %sysevalf(%superq(_last_mean)=., boolean) %then %do;
        %let _note=No fue posible calcular la diferencia relativa por datos faltantes.;
    %end;
    %else %if %sysevalf(&_first_mean.=0) %then %do;
        %let _note=Promedio inicial igual a 0; diferencia relativa no definida.;
    %end;

    proc fedsql sessref=conn;
        create table casuser._tgt_rel_row {options replace=true} as
        select min(Split) as Split,
            &_n_months. as N_Months,
            &_take. as Window_Size,
            "&_window_type." as Window_Type,
            "&_start_label." as Start_Label,
            &_first_mean. as Start_Value,
            "&_end_label." as End_Label,
            &_last_mean. as End_Value,
            %if %length(%superq(_note)) > 0 %then %do;
                . as Relative_Diff,
                "&_note." as Note
            %end;
            %else %do;
                ((&_last_mean.) - (&_first_mean.)) / (&_first_mean.) as Relative_Diff,
                '' as Note
            %end;
        from &monthly.
        where Split="&split.";
    quit;

    proc cas;
        session conn;
        table.append /
            source={caslib="casuser", name="_tgt_rel_row"},
            target={caslib="casuser", name="_tgt_rel_diff"};
        table.dropTable / caslib="casuser" name="_tgt_rel_row" quiet=true;
    quit;
%mend _target_rel_diff_split;

%macro _target_compute_rel_diff(monthly=casuser._tgt_rd_monthly,
    out=casuser._tgt_rel_diff);

    %_target_sort_cas(table_name=_tgt_rd_monthly, orderby={"Split","Periodo"});
    %_target_init_rel_diff(out=&out.);
    %_target_rel_diff_split(split=TRAIN, monthly=&monthly., out=&out.);
    %_target_rel_diff_split(split=OOT, monthly=&monthly., out=&out.);
    %_target_sort_cas(table_name=_tgt_rel_diff, orderby={"Split"});

%mend _target_compute_rel_diff;

%macro _target_compute_materiality(out=casuser._tgt_materiality);

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select Split,
            Periodo,
            Target as Target_Value,
            count(*) as N_Cuentas
        from casuser._tgt_all
        group by Split, Periodo, Target
        order by Split, Periodo, Target_Value;
    quit;

%mend _target_compute_materiality;

%macro _target_compute_bands(monthly=casuser._tgt_rd_monthly,
    out=casuser._tgt_bands);

    proc fedsql sessref=conn;
        create table casuser._tgt_band_stats {options replace=true} as
        select mean(RD) as Global_Avg,
            coalesce(stddev_samp(RD), 0) as Std_Monthly
        from &monthly.
        where Split='TRAIN';

        create table &out. {options replace=true} as
        select a.Split,
            a.Periodo,
            a.N_Total,
            a.N_Valid,
            a.N_Default,
            a.RD,
            b.Global_Avg,
            b.Std_Monthly,
            (b.Global_Avg - 2 * b.Std_Monthly) as Lower_Band,
            (b.Global_Avg + 2 * b.Std_Monthly) as Upper_Band,
            (b.Global_Avg - 3 * b.Std_Monthly) as Axis_Min,
            (b.Global_Avg + 3 * b.Std_Monthly) as Axis_Max
        from &monthly. a, casuser._tgt_band_stats b
        order by a.Split, a.Periodo;
    quit;

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="_tgt_band_stats" quiet=true;
    quit;

%mend _target_compute_bands;

%macro _target_compute_weighted_avg(out=casuser._tgt_weight_avg);

    proc fedsql sessref=conn;
        create table casuser._tgt_weight_avg_base {options replace=true} as
        select Split,
            Periodo,
            count(*) as N_Cuentas,
            sum(Monto) as Total_Monto,
            (sum(Target * Monto) / sum(Monto)) as RD_Pond_Prom
        from casuser._tgt_all
        where Monto > 0
        group by Split, Periodo;

        create table casuser._tgt_weight_avg_stats {options replace=true} as
        select mean(RD_Pond_Prom) as Global_Avg,
            coalesce(stddev_samp(RD_Pond_Prom), 0) as Std_Monthly
        from casuser._tgt_weight_avg_base
        where Split='TRAIN';

        create table &out. {options replace=true} as
        select a.Split,
            a.Periodo,
            a.N_Cuentas,
            a.Total_Monto,
            a.RD_Pond_Prom,
            b.Global_Avg,
            b.Std_Monthly,
            (b.Global_Avg - 2 * b.Std_Monthly) as Lower_Band,
            (b.Global_Avg + 2 * b.Std_Monthly) as Upper_Band,
            (b.Global_Avg - 5 * b.Std_Monthly) as Axis_Min,
            (b.Global_Avg + 5 * b.Std_Monthly) as Axis_Max
        from casuser._tgt_weight_avg_base a, casuser._tgt_weight_avg_stats b
        order by a.Split, a.Periodo;
    quit;

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="_tgt_weight_avg_base"
            quiet=true;
        table.dropTable / caslib="casuser" name="_tgt_weight_avg_stats"
            quiet=true;
    quit;

%mend _target_compute_weighted_avg;

%macro _target_compute_weighted_sum(out=casuser._tgt_weight_sum,
    out_ratio=casuser._tgt_weight_ratio);

    proc fedsql sessref=conn;
        create table casuser._tgt_weight_sum_base {options replace=true} as
        select Split,
            Periodo,
            count(*) as N_Cuentas,
            sum(Target * Monto) as Sum_Target_Pond,
            sum(Monto) as Total_Monto
        from casuser._tgt_all
        group by Split, Periodo;

        create table casuser._tgt_weight_sum_stats {options replace=true} as
        select mean(Sum_Target_Pond) as Global_Sum,
            coalesce(stddev_samp(Sum_Target_Pond), 0) as Std_Monthly_Sum
        from casuser._tgt_weight_sum_base
        where Split='TRAIN';

        create table &out. {options replace=true} as
        select a.Split,
            a.Periodo,
            a.N_Cuentas,
            a.Sum_Target_Pond,
            a.Total_Monto,
            b.Global_Sum,
            b.Std_Monthly_Sum,
            (b.Global_Sum - 2 * b.Std_Monthly_Sum) as Lower_Band,
            (b.Global_Sum + 2 * b.Std_Monthly_Sum) as Upper_Band
        from casuser._tgt_weight_sum_base a, casuser._tgt_weight_sum_stats b
        order by a.Split, a.Periodo;

        create table casuser._tgt_weight_ratio_base {options replace=true} as
        select Split,
            Periodo,
            N_Cuentas,
            Sum_Target_Pond,
            Total_Monto,
            case
                when Total_Monto = 0 then .
                else Sum_Target_Pond / Total_Monto
            end as Ratio_RD_Monto
        from casuser._tgt_weight_sum_base;

        create table casuser._tgt_weight_ratio_stats {options replace=true} as
        select mean(Ratio_RD_Monto) as Global_Ratio,
            coalesce(stddev_samp(Ratio_RD_Monto), 0) as Std_Ratio
        from casuser._tgt_weight_ratio_base
        where Split='TRAIN';

        create table &out_ratio. {options replace=true} as
        select a.Split,
            a.Periodo,
            a.N_Cuentas,
            a.Sum_Target_Pond,
            a.Total_Monto,
            a.Ratio_RD_Monto,
            b.Global_Ratio,
            b.Std_Ratio,
            (b.Global_Ratio - 2 * b.Std_Ratio) as Lower_Band,
            (b.Global_Ratio + 2 * b.Std_Ratio) as Upper_Band,
            (b.Global_Ratio - 5 * b.Std_Ratio) as Axis_Min,
            (b.Global_Ratio + 5 * b.Std_Ratio) as Axis_Max
        from casuser._tgt_weight_ratio_base a, casuser._tgt_weight_ratio_stats b
        order by a.Split, a.Periodo;
    quit;

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="_tgt_weight_sum_base"
            quiet=true;
        table.dropTable / caslib="casuser" name="_tgt_weight_sum_stats"
            quiet=true;
        table.dropTable / caslib="casuser" name="_tgt_weight_ratio_base"
            quiet=true;
        table.dropTable / caslib="casuser" name="_tgt_weight_ratio_stats"
            quiet=true;
    quit;

%mend _target_compute_weighted_sum;

%macro _target_compute(input_caslib=, train_table=, oot_table=, byvar=,
    target=, monto_var=, def_cld=, has_monto=0);

    %put NOTE: [target_compute] Preparando inputs filtrados en CAS.;
    %_target_prepare_inputs(input_caslib=&input_caslib.,
        train_table=&train_table., oot_table=&oot_table., byvar=&byvar.,
        target=&target., monto_var=&monto_var., def_cld=&def_cld.,
        has_monto=&has_monto.);

    %put NOTE: [target_compute] Calculando RD mensual.;
    %_target_compute_monthly_rd(out=casuser._tgt_rd_monthly);
    %_target_sort_cas(table_name=_tgt_rd_monthly, orderby={"Split","Periodo"});

    %put NOTE: [target_compute] Calculando diferencia relativa.;
    %_target_compute_rel_diff(monthly=casuser._tgt_rd_monthly,
        out=casuser._tgt_rel_diff);

    %put NOTE: [target_compute] Calculando materialidad.;
    %_target_compute_materiality(out=casuser._tgt_materiality);
    %_target_sort_cas(table_name=_tgt_materiality,
        orderby={"Split","Periodo","Target_Value"});

    %put NOTE: [target_compute] Calculando bandas del target.;
    %_target_compute_bands(monthly=casuser._tgt_rd_monthly,
        out=casuser._tgt_bands);
    %_target_sort_cas(table_name=_tgt_bands, orderby={"Split","Periodo"});

    %if &has_monto.=1 %then %do;
        %put NOTE: [target_compute] Calculando variantes ponderadas.;
        %_target_compute_weighted_avg(out=casuser._tgt_weight_avg);
        %_target_sort_cas(table_name=_tgt_weight_avg,
            orderby={"Split","Periodo"});
        %_target_compute_weighted_sum(out=casuser._tgt_weight_sum,
            out_ratio=casuser._tgt_weight_ratio);
        %_target_sort_cas(table_name=_tgt_weight_sum,
            orderby={"Split","Periodo"});
        %_target_sort_cas(table_name=_tgt_weight_ratio,
            orderby={"Split","Periodo"});
    %end;
    %else %do;
        %put NOTE: [target_compute] Analisis ponderados omitidos
            (has_monto=&has_monto.).;
    %end;

%mend _target_compute;
