/* =========================================================================
gini_variable_compute.sas - Gini de variables (general, comparativo y mensual)
========================================================================= */

%macro _gini_variables_general_split(data=, split=, target=, vars_num=,
    with_missing=1, min_n_valid=30, var_low=, var_high=,
    out=casuser._gini_vars_split);

    %local _gini_split_label;
    %let _gini_split_label=%upcase(%superq(split));

    %if %length(%superq(vars_num))=0 %then %do;
        data &out.;
            length Variable $64 Split $5 N_Total N_Valid N_Default N_Gini Ranking 8
                Pct_Valid Gini Smdcr_Raw 8 Evaluacion $15;
            format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
            stop;
        run;
        %return;
    %end;

    %_gini_profile_general(data=&data., split=&split., target=&target.,
        vars_num=&vars_num., with_missing=&with_missing.,
        out=work._gini_var_profile);

    data work._gini_var_gini;
        length Variable $64 Split $5 Smdcr_Raw Gini 8;
        format Gini Smdcr_Raw 8.4;
        stop;
    run;

    %local _i _var _n_gini_var _smdcr _gini_var_label;
    %let _i=1;
    %let _var=%scan(&vars_num., &_i., %str( ));

    %do %while(%length(&_var.) > 0);
        proc sql noprint;
            select max(N_Gini) into :_n_gini_var trimmed
            from work._gini_var_profile
            where Variable="%upcase(&_var.)";
        quit;

        %let _smdcr=.;
        %if %sysfunc(inputn(%superq(_n_gini_var), best32.)) >= &min_n_valid.
            %then %do;
            %_gini_freqtab_general(data=&data., target=&target., score=&_var.,
                with_missing=&with_missing., out=work._gini_var_ft);
            %if %sysfunc(exist(work._gini_var_ft)) %then %do;
                proc sql noprint;
                    select max(_SMDCR_) into :_smdcr trimmed
                    from work._gini_var_ft;
                quit;
            %end;

            %let _gini_var_label=%upcase(%superq(_var));

            data work._gini_var_row;
                length Variable $64 Split $5 Smdcr_Raw Gini 8;
                Variable=symget('_gini_var_label');
                Split=symget('_gini_split_label');
                Smdcr_Raw=input(symget('_smdcr'), best32.);
                Gini=abs(Smdcr_Raw);
                format Gini Smdcr_Raw 8.4;
            run;

            proc append base=work._gini_var_gini data=work._gini_var_row force;
            run;
        %end;

        proc datasets library=work nolist nowarn;
            delete _gini_var_ft _gini_var_row;
        quit;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(&vars_num., &_i., %str( ));
    %end;

    proc sql noprint;
        create table work._gini_vars_join as
        select p.Variable,
            p.Split,
            p.N_Total,
            p.N_Valid,
            p.N_Default,
            p.N_Gini,
            g.Smdcr_Raw,
            g.Gini
        from work._gini_var_profile p
        left join work._gini_var_gini g
            on p.Variable=g.Variable and p.Split=g.Split;
    quit;

    data work._gini_vars_ranked;
        length Evaluacion $15;
        set work._gini_vars_join;
        Ranking=.;
        if N_Total > 0 then Pct_Valid=N_Valid / N_Total;
        if N_Gini < &min_n_valid. then Evaluacion="MIN DATOS";
        else if missing(Gini) then Evaluacion="SIN DATOS";
        else if Gini >= &var_high. then Evaluacion="SATISFACTORIO";
        else if Gini >= &var_low. then Evaluacion="ACEPTABLE";
        else Evaluacion="BAJO";
        SortMissing=missing(Gini);
        format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
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
        delete _gini_var_ft _gini_var_gini _gini_var_profile _gini_vars_join
            _gini_vars_ranked;
    quit;

%mend _gini_variables_general_split;

%macro _gini_variables_general(train_data=, oot_data=, target=,
    vars_num_train=, vars_num_oot=, with_missing=1, min_n_valid=30, var_low=,
    var_high=, out=casuser._gini_vars_general);

    %if %length(%superq(vars_num_train))=0 and %length(%superq(vars_num_oot))>0
        %then %let vars_num_train=&vars_num_oot.;
    %if %length(%superq(vars_num_oot))=0 and %length(%superq(vars_num_train))>0
        %then %let vars_num_oot=&vars_num_train.;

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
        target=&target., vars_num=&vars_num_train., with_missing=&with_missing.,
        min_n_valid=&min_n_valid., var_low=&var_low., var_high=&var_high.,
        out=work._gini_vars_train);

    %_gini_variables_general_split(data=&oot_data., split=OOT,
        target=&target., vars_num=&vars_num_oot., with_missing=&with_missing.,
        min_n_valid=&min_n_valid., var_low=&var_low., var_high=&var_high.,
        out=work._gini_vars_oot);

    data &out.;
        set work._gini_vars_train work._gini_vars_oot;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_train _gini_vars_oot;
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
    out=casuser._gini_vars_month_split);

    %local _gini_split_label;
    %let _gini_split_label=%upcase(%superq(split));

    %if %length(%superq(vars_num))=0 %then %do;
        data &out.;
            length Variable $64 Split $5 Periodo 8 N_Total N_Valid N_Default N_Gini 8
                Gini Smdcr_Raw 8 Evaluacion $15;
            format Gini Smdcr_Raw 8.4;
            stop;
        run;
        %return;
    %end;

    %_gini_profile_monthly(data=&data., split=&split., target=&target.,
        vars_num=&vars_num., byvar=&byvar., with_missing=&with_missing.,
        out=work._gini_var_profile_m);

    data work._gini_vars_month_gini;
        length Variable $64 Split $5 Periodo 8 Smdcr_Raw Gini 8;
        format Gini Smdcr_Raw 8.4;
        stop;
    run;

    %local _i _var _gini_var_label _n_gini_var;
    %let _i=1;
    %let _var=%scan(&vars_num., &_i., %str( ));

    %do %while(%length(&_var.) > 0);
        %let _gini_var_label=%upcase(%superq(_var));

        proc sql noprint;
            select max(N_Gini) into :_n_gini_var trimmed
            from work._gini_var_profile_m
            where Variable="%upcase(&_var.)";
        quit;

        %if %sysfunc(inputn(%superq(_n_gini_var), best32.)) >= &min_n_valid.
            %then %do;
            %if &with_missing.=1 %then %do;
            proc freqtab data=&data. noprint missing;
                by &byvar.;
                tables &target. * &_var. / measures;
                output out=work._gini_var_ftb smdcr;
            run;
            %end;
            %else %do;
            proc freqtab data=&data. noprint;
                by &byvar.;
                tables &target. * &_var. / measures;
                output out=work._gini_var_ftb smdcr;
            run;
            %end;

            data work._gini_var_row;
            length Variable $64 Split $5 Periodo 8 Smdcr_Raw Gini 8;
            set work._gini_var_ftb;
            Periodo=&byvar.;
            Variable=symget('_gini_var_label');
            Split=symget('_gini_split_label');
            Smdcr_Raw=_SMDCR_;
            Gini=abs(_SMDCR_);
            format Gini Smdcr_Raw 8.4;
            keep Variable Split Periodo Smdcr_Raw Gini;
        run;

        proc append base=work._gini_vars_month_gini data=work._gini_var_row
            force;
        run;
        %end;

        proc datasets library=work nolist nowarn;
            delete _gini_var_ftb _gini_var_row;
        quit;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(&vars_num., &_i., %str( ));
    %end;

    proc sql noprint;
        create table work._gini_vars_month_join as
        select p.Variable,
            p.Split,
            p.Periodo,
            p.N_Total,
            p.N_Valid,
            p.N_Default,
            p.N_Gini,
            g.Smdcr_Raw,
            g.Gini
        from work._gini_var_profile_m p
        left join work._gini_vars_month_gini g
            on p.Variable=g.Variable and p.Split=g.Split and
            p.Periodo=g.Periodo;
    quit;

    data &out.;
        length Evaluacion $15;
        set work._gini_vars_month_join;
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

    proc datasets library=work nolist nowarn;
        delete _gini_var_ftb _gini_var_profile_m _gini_vars_month_gini
            _gini_vars_month_join;
    quit;

%mend _gini_variables_monthly_split;

%macro _gini_variables_monthly(train_data=, oot_data=, target=,
    vars_num_train=, vars_num_oot=, byvar=, with_missing=1, min_n_valid=30,
    var_low=, var_high=, out=casuser._gini_vars_detail);

    %if %length(%superq(vars_num_train))=0 and %length(%superq(vars_num_oot))>0
        %then %let vars_num_train=&vars_num_oot.;
    %if %length(%superq(vars_num_oot))=0 and %length(%superq(vars_num_train))>0
        %then %let vars_num_oot=&vars_num_train.;

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

    data &out.;
        set work._gini_vars_trn_m work._gini_vars_oot_m;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_trn_m _gini_vars_oot_m;
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
