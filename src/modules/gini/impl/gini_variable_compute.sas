/* =========================================================================
gini_variable_compute.sas - Gini de variables (general, comparativo y mensual)
========================================================================= */

%macro _gini_variables_general_split(data=, split=, target=, vars_num=,
    with_missing=1, min_n_valid=30, var_low=, var_high=,
    out=work._gini_vars_split);

    data &out.;
        length Variable $64 Split $5 N_Total N_Valid N_Default N_Gini Ranking 8
            Pct_Valid Gini Smdcr_Raw 8 Evaluacion $15;
        format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
        stop;
    run;

    %if %length(%superq(vars_num))=0 %then %return;

    %local _i _var _n_total _n_valid _n_default _n_gini _smdcr;
    %let _i=1;
    %let _var=%scan(&vars_num., &_i., %str( ));

    %do %while(%length(&_var.) > 0);
        proc sql noprint;
            select count(*),
                sum(case when not missing(&_var.) then 1 else 0 end),
                sum(&target.)
            into :_n_total trimmed, :_n_valid trimmed, :_n_default trimmed
            from &data.;
        quit;

        %_gini_count_rows(data=&data., target=&target., score=&_var.,
            with_missing=&with_missing., outvar=_n_gini);

        %let _smdcr=.;
        %if %sysfunc(inputn(&_n_gini., best32.)) >= &min_n_valid. %then %do;
            %_gini_freqtab_general(data=&data., target=&target.,
                score=&_var., with_missing=&with_missing.,
                out=work._gini_var_ft);
            %if %sysfunc(exist(work._gini_var_ft)) %then %do;
                proc sql noprint;
                    select max(_SMDCR_) into :_smdcr trimmed
                    from work._gini_var_ft;
                quit;
            %end;
        %end;

        data work._gini_var_row;
            length Variable $64 Split $5 Evaluacion $15;
            Variable="%upcase(&_var.)";
            Split="&split.";
            N_Total=input(symget('_n_total'), best32.);
            N_Valid=input(symget('_n_valid'), best32.);
            N_Default=input(symget('_n_default'), best32.);
            N_Gini=input(symget('_n_gini'), best32.);
            Ranking=.;
            if N_Total > 0 then Pct_Valid=N_Valid / N_Total;
            Smdcr_Raw=input(symget('_smdcr'), best32.);
            Gini=abs(Smdcr_Raw);
            if N_Gini < &min_n_valid. then Evaluacion="MIN DATOS";
            else if missing(Gini) then Evaluacion="SIN DATOS";
            else if Gini >= &var_high. then Evaluacion="SATISFACTORIO";
            else if Gini >= &var_low. then Evaluacion="ACEPTABLE";
            else Evaluacion="BAJO";
            format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
        run;

        proc append base=&out. data=work._gini_var_row force;
        run;

        proc datasets library=work nolist nowarn;
            delete _gini_var_ft _gini_var_row;
        quit;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(&vars_num., &_i., %str( ));
    %end;

    data work._gini_vars_ranked;
        set &out.;
        SortMissing=missing(Gini);
    run;

    proc sort data=work._gini_vars_ranked;
        by SortMissing descending Gini Variable;
    run;

    data &out.;
        set work._gini_vars_ranked;
        retain _rank 0;
        if _n_=1 then _rank=0;
        if SortMissing=0 then do;
            _rank + 1;
            Ranking=_rank;
        end;
        else Ranking=.;
        drop _rank SortMissing;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_ranked;
    quit;

%mend _gini_variables_general_split;

%macro _gini_variables_general(train_data=, oot_data=, target=,
    vars_num_train=, vars_num_oot=, with_missing=1, min_n_valid=30, var_low=,
    var_high=,
    out=casuser._gini_vars_general);

    %if %length(%superq(vars_num_train))=0 and %length(%superq(vars_num_oot))=0
        %then %do;
        data &out.;
            length Variable $64 Split $5 N_Total N_Valid N_Default N_Gini Ranking 8
                Pct_Valid Gini Smdcr_Raw 8 Evaluacion $15;
            format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
            stop;
        run;
        %return;
    %end;

    %_gini_variables_general_split(data=&train_data., split=TRAIN,
        target=&target., vars_num=&vars_num_train.,
        with_missing=&with_missing.,
        min_n_valid=&min_n_valid., var_low=&var_low., var_high=&var_high.,
        out=work._gini_vars_train);

    %_gini_variables_general_split(data=&oot_data., split=OOT,
        target=&target., vars_num=&vars_num_oot., with_missing=&with_missing.,
        min_n_valid=&min_n_valid., var_low=&var_low., var_high=&var_high.,
        out=work._gini_vars_oot);

    data work._gini_vars_all;
        set work._gini_vars_train work._gini_vars_oot;
    run;

    data &out.;
        set work._gini_vars_all;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_train _gini_vars_oot _gini_vars_all;
    quit;

%mend _gini_variables_general;

%macro _gini_variables_compare(data=, delta_warn=0.05,
    out=casuser._gini_vars_compare);

    data work._gini_vars_general_w;
        set &data.;
    run;

    proc sql noprint;
        create table work._gini_vars_compare_tmp as
        select coalesce(t.Variable, o.Variable) as Variable length=64,
            t.Gini as Gini_Train format=8.4,
            t.Ranking as Rank_Train,
            o.Gini as Gini_OOT format=8.4,
            o.Ranking as Rank_OOT,
            (t.Gini - o.Gini) as Delta_Gini format=8.4,
            abs(t.Ranking - o.Ranking) as Delta_Rank,
            case
                when t.Gini is missing or o.Gini is missing then "SIN DATOS"
                when calculated Delta_Gini > &delta_warn. then "DEGRADACION"
                when calculated Delta_Gini < -&delta_warn. then "MEJORA"
                else "ESTABLE"
            end as Estabilidad length=15
        from (select * from work._gini_vars_general_w where Split="TRAIN") t
        full join (select * from work._gini_vars_general_w where Split="OOT") o
            on t.Variable=o.Variable;
    quit;

    data &out.;
        set work._gini_vars_compare_tmp;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_general_w _gini_vars_compare_tmp;
    quit;

%mend _gini_variables_compare;

%macro _gini_variables_monthly_split(data=, split=, target=, vars_num=,
    byvar=, with_missing=1, min_n_valid=30, var_low=, var_high=,
    out=work._gini_vars_month_split);

    data &out.;
        length Variable $64 Split $5 Periodo 8 N_Total N_Valid N_Default N_Gini 8
            Gini Smdcr_Raw 8 Evaluacion $15;
        format Gini Smdcr_Raw 8.4;
        stop;
    run;

    %if %length(%superq(vars_num))=0 %then %return;

    data work._gini_var_src;
        set &data.;
    run;

    proc sort data=work._gini_var_src;
        by &byvar.;
    run;

    %local _i _var;
    %let _i=1;
    %let _var=%scan(&vars_num., &_i., %str( ));

    %do %while(%length(&_var.) > 0);
        proc sql noprint;
            create table work._gini_var_cnt as
            select "%upcase(&_var.)" as Variable length=64,
                "&split." as Split length=5,
                &byvar. as Periodo,
                count(*) as N_Total,
                sum(case when not missing(&_var.) then 1 else 0 end)
                    as N_Valid,
                sum(&target.) as N_Default
            from work._gini_var_src
            group by &byvar.;
        quit;

        %if &with_missing.=1 %then %do;
            proc sql noprint;
                create table work._gini_var_ng as
                select &byvar. as Periodo,
                    count(*) as N_Gini
                from work._gini_var_src
                where not missing(&target.)
                group by &byvar.;
            quit;

            proc freqtab data=work._gini_var_src noprint missing;
                by &byvar.;
                tables &target. * &_var. / measures;
                output out=work._gini_var_ftb smdcr;
            run;
        %end;
        %else %do;
            proc sql noprint;
                create table work._gini_var_ng as
                select &byvar. as Periodo,
                    count(*) as N_Gini
                from work._gini_var_src
                where not missing(&target.) and not missing(&_var.)
                group by &byvar.;
            quit;

            proc freqtab data=work._gini_var_src noprint;
                by &byvar.;
                tables &target. * &_var. / measures;
                output out=work._gini_var_ftb smdcr;
            run;
        %end;

        proc sql noprint;
            create table work._gini_var_row as
            select c.Variable,
                c.Split,
                c.Periodo,
                c.N_Total,
                c.N_Valid,
                c.N_Default,
                n.N_Gini,
                f._SMDCR_ as Smdcr_Raw format=8.4,
                abs(f._SMDCR_) as Gini format=8.4
            from work._gini_var_cnt c
            left join work._gini_var_ng n
                on c.Periodo=n.Periodo
            left join work._gini_var_ftb f
                on c.Periodo=f.&byvar.;
        quit;

        data work._gini_var_row;
            set work._gini_var_row;
            length Evaluacion $15;
            if N_Gini < &min_n_valid. then do;
                Smdcr_Raw=.;
                Gini=.;
                Evaluacion="MIN DATOS";
            end;
            else if missing(Gini) then Evaluacion="SIN DATOS";
            else if Gini >= &var_high. then Evaluacion="SATISFACTORIO";
            else if Gini >= &var_low. then Evaluacion="ACEPTABLE";
            else Evaluacion="BAJO";
            format Gini Smdcr_Raw 8.4;
        run;

        proc append base=&out. data=work._gini_var_row force;
        run;

        proc datasets library=work nolist nowarn;
            delete _gini_var_cnt _gini_var_ng _gini_var_ftb _gini_var_row;
        quit;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(&vars_num., &_i., %str( ));
    %end;

    proc datasets library=work nolist nowarn;
        delete _gini_var_src;
    quit;

%mend _gini_variables_monthly_split;

%macro _gini_variables_monthly(train_data=, oot_data=, target=,
    vars_num_train=, vars_num_oot=, byvar=, with_missing=1, min_n_valid=30,
    var_low=, var_high=,
    out=casuser._gini_vars_detail);

    %if %length(%superq(vars_num_train))=0 and %length(%superq(vars_num_oot))=0
        %then %do;
        data &out.;
            length Variable $64 Split $5 Periodo 8 N_Total N_Valid N_Default N_Gini 8
                Gini Smdcr_Raw 8 Evaluacion $15;
            format Gini Smdcr_Raw 8.4;
            stop;
        run;
        %return;
    %end;

    %_gini_variables_monthly_split(data=&train_data., split=TRAIN,
        target=&target., vars_num=&vars_num_train., byvar=&byvar.,
        with_missing=&with_missing., min_n_valid=&min_n_valid.,
        var_low=&var_low., var_high=&var_high., out=work._gini_vars_trn_m);

    %_gini_variables_monthly_split(data=&oot_data., split=OOT,
        target=&target., vars_num=&vars_num_oot., byvar=&byvar.,
        with_missing=&with_missing., min_n_valid=&min_n_valid.,
        var_low=&var_low., var_high=&var_high., out=work._gini_vars_oot_m);

    data work._gini_vars_monthly_all;
        set work._gini_vars_trn_m work._gini_vars_oot_m;
    run;

    data &out.;
        set work._gini_vars_monthly_all;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_trn_m _gini_vars_oot_m _gini_vars_monthly_all;
    quit;

%mend _gini_variables_monthly;

%macro _gini_variables_summary(data=, var_low=, var_high=, trend_delta=0.03,
    out=casuser._gini_vars_summary);

    data work._gini_vars_detail_w;
        set &data.;
    run;

    proc sql noprint;
        create table work._gini_vars_base as
        select Variable,
            Split,
            count(*) as N_Periodos,
            min(Periodo) as First_Period,
            max(Periodo) as Last_Period,
            mean(Gini) as Gini_Promedio format=8.4,
            min(Gini) as Gini_Min format=8.4,
            max(Gini) as Gini_Max format=8.4,
            std(Gini) as Gini_Std format=8.4
        from work._gini_vars_detail_w
        where Gini is not missing
        group by Variable, Split;

        create table work._gini_vars_summary_tmp as
        select a.Variable,
            a.Split,
            a.N_Periodos,
            a.First_Period,
            a.Last_Period,
            f.Gini as Gini_First format=8.4,
            l.Gini as Gini_Last format=8.4,
            a.Gini_Promedio,
            a.Gini_Min,
            a.Gini_Max,
            a.Gini_Std,
            (l.Gini - f.Gini) as Delta_Gini format=8.4,
            case
                when f.Gini is missing or l.Gini is missing then "SIN DATOS"
                when calculated Delta_Gini < -&trend_delta. then "EMPEORANDO"
                when calculated Delta_Gini > &trend_delta. then "MEJORANDO"
                else "ESTABLE"
            end as Tendencia length=15,
            case
                when a.Gini_Promedio is missing then "SIN DATOS"
                when a.Gini_Promedio >= &var_high. then "SATISFACTORIO"
                when a.Gini_Promedio >= &var_low. then "ACEPTABLE"
                else "BAJO"
            end as Evaluacion length=15
        from work._gini_vars_base a
        left join work._gini_vars_detail_w f
            on a.Variable=f.Variable and a.Split=f.Split and
            a.First_Period=f.Periodo
        left join work._gini_vars_detail_w l
            on a.Variable=l.Variable and a.Split=l.Split and
            a.Last_Period=l.Periodo;
    quit;

    data &out.;
        set work._gini_vars_summary_tmp;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_base _gini_vars_detail_w _gini_vars_summary_tmp;
    quit;

%mend _gini_variables_summary;
