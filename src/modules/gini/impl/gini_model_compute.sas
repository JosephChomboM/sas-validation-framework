/* =========================================================================
gini_model_compute.sas - Gini del modelo (general y mensual)
========================================================================= */

%macro _gini_model_general_split(data=, split=, split_var=Split, target=,
    score=, with_missing=1, model_low=, model_high=,
    out=casuser._gini_model_split);

    %local _gini_n _smdcr _gini_ft_exists;

    proc fedsql sessref=conn;
        create table casuser._gini_model_src {options replace=true} as
        select *
        from &data.
        where &split_var.='&split.';
    quit;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select '&split.' as Split,
            count(*) as N_Total,
            sum(&target.) as N_Default,
            sum(1-&target.) as N_No_Default,
            (sum(&target.) / count(*)) as Tasa_Default
        from casuser._gini_model_src;
    quit;

    %_gini_count_rows(
        data=casuser._gini_model_src,
        target=&target.,
        score=&score.,
        with_missing=&with_missing.,
        outvar=_gini_n
    );
    %let _smdcr=.;

    %_gini_freqtab_general(
        data=casuser._gini_model_src,
        target=&target.,
        score=&score.,
        with_missing=&with_missing.,
        out=casuser._gini_model_ft
    );

    %let _gini_ft_exists=0;
    proc sql noprint;
        select count(*) into :_gini_ft_exists trimmed
        from dictionary.tables
        where upcase(libname)='CASUSER'
          and upcase(memname)='_GINI_MODEL_FT';
    quit;

    %if &_gini_ft_exists. > 0 %then %do;
        proc sql noprint;
            select max(_SMDCR_) into :_smdcr trimmed
            from casuser._gini_model_ft;
        quit;
    %end;

    data &out.;
        set &out.;
        length Evaluacion $15;
        N_Gini=input(symget('_gini_n'), best32.);
        Smdcr_Raw=input(symget('_smdcr'), best32.);
        Gini=abs(Smdcr_Raw);
        if N_Gini > 0 and not missing(Gini) then do;
            SE=sqrt(Gini * (1 - Gini) / N_Gini);
            IC_95_Lower=max(0, Gini - 1.96 * SE);
            IC_95_Upper=min(1, Gini + 1.96 * SE);
        end;
        if missing(Gini) then Evaluacion="SIN DATOS";
        else if Gini >= &model_high. then Evaluacion="SATISFACTORIO";
        else if Gini >= &model_low. then Evaluacion="ACEPTABLE";
        else Evaluacion="BAJO";
        format Tasa_Default percent8.2 Gini Smdcr_Raw SE IC_95_Lower
            IC_95_Upper 8.4;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_src _gini_model_ft;
    quit;

%mend _gini_model_general_split;

%macro _gini_model_general(data=, split_var=Split, target=, score=,
    with_missing=1, model_low=, model_high=, out=casuser._gini_model_general);

    %_gini_model_general_split(data=&data., split=TRAIN, split_var=&split_var.,
        target=&target., score=&score., with_missing=&with_missing.,
        model_low=&model_low., model_high=&model_high.,
        out=casuser._gini_model_train);

    %_gini_model_general_split(data=&data., split=OOT, split_var=&split_var.,
        target=&target., score=&score., with_missing=&with_missing.,
        model_low=&model_low., model_high=&model_high.,
        out=casuser._gini_model_oot);

    data &out.;
        set casuser._gini_model_train casuser._gini_model_oot;
    run;

    %local _gini_train;
    %let _gini_train=.;
    proc sql noprint;
        select Gini into :_gini_train trimmed
        from &out.
        where Split='TRAIN';
    quit;

    data &out.;
        set &out.;
        if Split='OOT' and not missing(Gini) and input(symget('_gini_train'),
            best32.) > 0 then Degradacion=
            (input(symget('_gini_train'), best32.) - Gini) /
            input(symget('_gini_train'), best32.);
        format Degradacion percent8.2;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_train _gini_model_oot;
    quit;

%mend _gini_model_general;

%macro _gini_model_monthly_split(data=, split=, split_var=Split, target=,
    score=, byvar=, with_missing=1, model_low=, model_high=,
    out=casuser._gini_model_month_split);

    proc fedsql sessref=conn;
        create table casuser._gini_model_src {options replace=true} as
        select *
        from &data.
        where &split_var.='&split.';
    quit;

    %_gini_sort_cas(table_name=_gini_model_src,
        orderby=%str({"&byvar."}));

    proc fedsql sessref=conn;
        create table casuser._gini_model_cnt {options replace=true} as
        select '&split.' as Split,
            &byvar. as Periodo,
            count(*) as N_Total,
            sum(&target.) as N_Default,
            sum(1-&target.) as N_No_Default,
            (sum(&target.) / count(*)) as Tasa_Default
        from casuser._gini_model_src
        group by &byvar.;
    quit;

    %if &with_missing.=1 %then %do;
        proc fedsql sessref=conn;
            create table casuser._gini_model_ng {options replace=true} as
            select &byvar. as Periodo,
                count(*) as N_Gini
            from casuser._gini_model_src
            where &target. is not null
            group by &byvar.;
        quit;

        proc freqtab data=casuser._gini_model_src noprint missing;
            by &byvar.;
            tables &target. * &score. / measures;
            output out=casuser._gini_model_ftb smdcr;
        run;
    %end;
    %else %do;
        proc fedsql sessref=conn;
            create table casuser._gini_model_ng {options replace=true} as
            select &byvar. as Periodo,
                count(*) as N_Gini
            from casuser._gini_model_src
            where &target. is not null and &score. is not null
            group by &byvar.;
        quit;

        proc freqtab data=casuser._gini_model_src noprint;
            by &byvar.;
            tables &target. * &score. / measures;
            output out=casuser._gini_model_ftb smdcr;
        run;
    %end;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select c.Split,
            c.Periodo,
            c.N_Total,
            c.N_Default,
            c.N_No_Default,
            c.Tasa_Default,
            n.N_Gini,
            f._SMDCR_ as Smdcr_Raw,
            abs(f._SMDCR_) as Gini
        from casuser._gini_model_cnt c
        left join casuser._gini_model_ng n
            on c.Periodo=n.Periodo
        left join casuser._gini_model_ftb f
            on c.Periodo=f.&byvar.;
    quit;

    data &out.;
        set &out.;
        length Evaluacion $15;
        if missing(Gini) then Evaluacion='SIN DATOS';
        else if Gini >= &model_high. then Evaluacion='SATISFACTORIO';
        else if Gini >= &model_low. then Evaluacion='ACEPTABLE';
        else Evaluacion='BAJO';
        format Tasa_Default percent8.2 Gini Smdcr_Raw 8.4;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_src _gini_model_cnt _gini_model_ng _gini_model_ftb;
    quit;

%mend _gini_model_monthly_split;

%macro _gini_model_monthly(data=, split_var=Split, target=, score=, byvar=,
    with_missing=1, model_low=, model_high=, trend_delta=0.03,
    out=casuser._gini_model_monthly);

    %_gini_model_monthly_split(data=&data., split=TRAIN, split_var=&split_var.,
        target=&target., score=&score., byvar=&byvar.,
        with_missing=&with_missing., model_low=&model_low.,
        model_high=&model_high., out=casuser._gini_model_train_m);

    %_gini_model_monthly_split(data=&data., split=OOT, split_var=&split_var.,
        target=&target., score=&score., byvar=&byvar.,
        with_missing=&with_missing., model_low=&model_low.,
        model_high=&model_high., out=casuser._gini_model_oot_m);

    data casuser._gini_model_monthly_all;
        set casuser._gini_model_train_m casuser._gini_model_oot_m;
    run;

    proc fedsql sessref=conn;
        create table casuser._gini_model_fl {options replace=true} as
        select Split,
            min(Periodo) as First_Period,
            max(Periodo) as Last_Period
        from casuser._gini_model_monthly_all
        group by Split;

        create table casuser._gini_model_trend {options replace=true} as
        select a.Split,
            f.Gini as Gini_First,
            l.Gini as Gini_Last,
            (l.Gini - f.Gini) as Delta_Gini
        from casuser._gini_model_fl a
        left join casuser._gini_model_monthly_all f
            on a.Split=f.Split and a.First_Period=f.Periodo
        left join casuser._gini_model_monthly_all l
            on a.Split=l.Split and a.Last_Period=l.Periodo;

        create table casuser._gini_model_monthly_out {options replace=true} as
        select a.*,
            t.Gini_First,
            t.Gini_Last,
            t.Delta_Gini,
            case
                when t.Gini_First is null or t.Gini_Last is null
                    then 'SIN DATOS'
                when t.Delta_Gini < -&trend_delta. then 'EMPEORANDO'
                when t.Delta_Gini > &trend_delta. then 'MEJORANDO'
                else 'ESTABLE'
            end as Tendencia
        from casuser._gini_model_monthly_all a
        left join casuser._gini_model_trend t
            on a.Split=t.Split;
    quit;

    data &out.;
        set casuser._gini_model_monthly_out;
        format Gini_First Gini_Last Delta_Gini Gini Smdcr_Raw 8.4
            Tasa_Default percent8.2;
        length Tendencia $15;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_train_m _gini_model_oot_m _gini_model_monthly_all
            _gini_model_fl _gini_model_trend _gini_model_monthly_out;
    quit;

%mend _gini_model_monthly;
