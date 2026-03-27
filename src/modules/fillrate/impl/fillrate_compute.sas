/* =========================================================================
fillrate_compute.sas - Computo para Fillrate vs Gini (optimizado)

Macros:
- %_fill_prepare_stage  : staging unico (normaliza sentinelas numericos)
- %_fill_general_compute: fillrate general + Gini global + Gini temporal
- %_fill_monthly_compute: fillrate mensual por variable
- %_fill_plot_monthly   : graficos de tendencia mensual

Notas:
- Se preserva la semantica vigente de Gini (_SMDCR_, sin option MISSING).
- Fillrate numerico trata sentinelas como no llenados (missing).
- Fillrate categorico usa not missing(cats(var)).
========================================================================= */

%macro _fill_prepare_stage(data=, vars_num=, vars_cat=, byvar=, target=,
    out=work._fill_stage);

    data &out.;
        set &data.(keep=Muestra &byvar. &target. &vars_num. &vars_cat.);

        %if %length(%superq(vars_num)) > 0 %then %do;
            array _fill_num {*} &vars_num.;
            do _fill_i=1 to dim(_fill_num);
                if _fill_num[_fill_i] in (., 1111111111, -1111111111,
                    2222222222, -2222222222, 3333333333, -3333333333,
                    4444444444, 5555555555, 6666666666, 7777777777,
                    -999999999) then _fill_num[_fill_i]=.;
            end;
            drop _fill_i;
        %end;
    run;

%mend _fill_prepare_stage;

%macro _fill_general_compute(data=, vars_num=, target=, byvar=,
    out=work._fill_general, out_bytime=work._fill_gini_bytime);

    data &out.;
        length Variable $64 Var_Type $8 Muestra $5 N_Total N_Filled N_Gini 8
            Fillrate Gini Smdcr_Raw 8;
        format Fillrate 8.2 Gini 8.4 Smdcr_Raw 8.4;
        stop;
    run;

    data &out_bytime.;
        length Variable $64 Var_Type $8 Muestra $5 &byvar. 8 N_Gini 8
            Smdcr_Raw Gini 8;
        format Gini 8.4 Smdcr_Raw 8.4;
        stop;
    run;

    %if %length(%superq(vars_num))=0 %then %return;

    proc sort data=&data. out=work._fill_stage_gini;
        by Muestra &byvar.;
    run;

    proc summary data=work._fill_stage_gini nway;
        class Muestra;
        output out=work._fill_gen_tot(drop=_type_ _freq_) n=N_Total;
    run;

    proc summary data=work._fill_stage_gini nway;
        class Muestra;
        var &vars_num.;
        output out=work._fill_gen_n(drop=_type_ _freq_) n=;
    run;

    proc sort data=work._fill_gen_tot;
        by Muestra;
    run;
    proc sort data=work._fill_gen_n;
        by Muestra;
    run;

    data work._fill_gen_base;
        merge work._fill_gen_tot(in=_a) work._fill_gen_n(in=_b);
        by Muestra;
        if not (_a and _b) then delete;

        length Variable $64 Var_Type $8;
        array _nfilled {*} &vars_num.;

        Var_Type='NUM';
        do _i=1 to dim(_nfilled);
            Variable=upcase(vname(_nfilled[_i]));
            N_Filled=_nfilled[_i];
            Fillrate=(N_Filled / N_Total) * 100;
            output;
        end;

        keep Variable Var_Type Muestra N_Total N_Filled Fillrate;
    run;

    data work._fill_gini_global;
        length Variable $64 Var_Type $8 Muestra $5 N_Gini 8 Smdcr_Raw Gini 8;
        format Gini 8.4 Smdcr_Raw 8.4;
        stop;
    run;

    data &out_bytime.;
        set &out_bytime.(obs=0);
        stop;
    run;

    %local _i _v _nvnum;
    %let _i=1;
    %let _v=%scan(%superq(vars_num), &_i., %str( ));

    %do %while(%length(%superq(_v)) > 0);
        %put NOTE: [fillrate] Gini global/temporal para &_v.;

        proc freqtab data=work._fill_stage_gini noprint;
            by Muestra;
            tables &target. * &_v. / measures;
            output out=work._fill_gini_tmp smdcr;
        run;

        proc sql noprint;
            create table work._fill_ngini_tmp as
            select upcase(Muestra) as Muestra length=5,
                   count(*) as N_Gini
            from work._fill_stage_gini
            where not missing(&target.)
              and not missing(&_v.)
            group by Muestra;
        quit;

        %if %sysfunc(exist(work._fill_gini_tmp)) %then %do;
            proc sql noprint;
                create table work._fill_gini_var as
                select upcase(a.Muestra) as Muestra length=5,
                       b.N_Gini,
                       max(a._SMDCR_) as Smdcr_Raw
                from work._fill_gini_tmp a
                left join work._fill_ngini_tmp b
                  on upcase(a.Muestra)=upcase(b.Muestra)
                group by upcase(a.Muestra), b.N_Gini;
            quit;
        %end;
        %else %do;
            data work._fill_gini_var;
                set work._fill_ngini_tmp;
                length Muestra $5;
                Smdcr_Raw=.;
            run;
        %end;

        data work._fill_gini_var;
            set work._fill_gini_var;
            length Variable $64 Var_Type $8;
            Variable=upcase("&_v.");
            Var_Type='NUM';
            Gini=abs(Smdcr_Raw);
            keep Variable Var_Type Muestra N_Gini Smdcr_Raw Gini;
        run;

        proc append base=work._fill_gini_global data=work._fill_gini_var force;
        run;

        proc freqtab data=work._fill_stage_gini noprint;
            by Muestra &byvar.;
            tables &target. * &_v. / measures;
            output out=work._fill_gini_bt_tmp smdcr;
        run;

        proc sql noprint;
            create table work._fill_ngini_bt_tmp as
            select upcase(Muestra) as Muestra length=5,
                   &byvar.,
                   count(*) as N_Gini
            from work._fill_stage_gini
            where not missing(&target.)
              and not missing(&_v.)
            group by upcase(Muestra), &byvar.;
        quit;

        %if %sysfunc(exist(work._fill_gini_bt_tmp)) %then %do;
            proc sql noprint;
                create table work._fill_gini_bt_var as
                select upcase(a.Muestra) as Muestra length=5,
                       a.&byvar.,
                       b.N_Gini,
                       max(a._SMDCR_) as Smdcr_Raw
                from work._fill_gini_bt_tmp a
                left join work._fill_ngini_bt_tmp b
                  on upcase(a.Muestra)=upcase(b.Muestra)
                 and a.&byvar.=b.&byvar.
                group by upcase(a.Muestra), a.&byvar., b.N_Gini;
            quit;
        %end;
        %else %do;
            data work._fill_gini_bt_var;
                set work._fill_ngini_bt_tmp;
                length Muestra $5;
                Smdcr_Raw=.;
            run;
        %end;

        data work._fill_gini_bt_var;
            set work._fill_gini_bt_var;
            length Variable $64 Var_Type $8;
            Variable=upcase("&_v.");
            Var_Type='NUM';
            Gini=abs(Smdcr_Raw);
            keep Variable Var_Type Muestra &byvar. N_Gini Smdcr_Raw Gini;
        run;

        proc append base=&out_bytime. data=work._fill_gini_bt_var force;
        run;

        proc datasets library=work nolist nowarn;
            delete _fill_gini_tmp _fill_ngini_tmp _fill_gini_var
                   _fill_gini_bt_tmp _fill_ngini_bt_tmp _fill_gini_bt_var;
        quit;

        %let _i=%eval(&_i. + 1);
        %let _v=%scan(%superq(vars_num), &_i., %str( ));
    %end;

    proc sql noprint;
        create table &out. as
        select a.Variable,
               a.Var_Type,
               upcase(a.Muestra) as Muestra length=5,
               a.N_Total,
               a.N_Filled,
               a.Fillrate,
               b.N_Gini,
               b.Smdcr_Raw,
               b.Gini
        from work._fill_gen_base a
        left join work._fill_gini_global b
          on upcase(a.Muestra)=upcase(b.Muestra)
         and upcase(a.Variable)=upcase(b.Variable)
        order by a.Variable, calculated Muestra;
    quit;

    proc sort data=&out_bytime.;
        by Variable Muestra &byvar.;
    run;

    proc datasets library=work nolist nowarn;
        delete _fill_gen_tot _fill_gen_n _fill_gen_base _fill_gini_global
               _fill_stage_gini;
    quit;

%mend _fill_general_compute;

%macro _fill_monthly_compute(data=, byvar=, vars_num=, vars_cat=,
    out=work._fill_monthly);

    %local _cidx _cvar;

    data &out.;
        length Variable $64 Var_Type $8 Muestra $5 &byvar. 8
            N_Total N_Filled 8 Fillrate 8;
        format Fillrate 8.2;
        stop;
    run;

    proc summary data=&data. nway;
        class Muestra &byvar.;
        output out=work._fill_mon_tot(drop=_type_ _freq_) n=N_Total;
    run;

    %if %length(%superq(vars_num)) > 0 %then %do;
        proc summary data=&data. nway;
            class Muestra &byvar.;
            var &vars_num.;
            output out=work._fill_mon_num(drop=_type_ _freq_) n=;
        run;

        proc sort data=work._fill_mon_tot;
            by Muestra &byvar.;
        run;
        proc sort data=work._fill_mon_num;
            by Muestra &byvar.;
        run;

        data work._fill_mon_num_long;
            merge work._fill_mon_tot(in=_a) work._fill_mon_num(in=_b);
            by Muestra &byvar.;
            if not (_a and _b) then delete;

            length Variable $64 Var_Type $8;
            array _nfilled {*} &vars_num.;
            Var_Type='NUM';

            do _i=1 to dim(_nfilled);
                Variable=upcase(vname(_nfilled[_i]));
                N_Filled=_nfilled[_i];
                Fillrate=(N_Filled / N_Total) * 100;
                output;
            end;

            keep Variable Var_Type Muestra &byvar. N_Total N_Filled Fillrate;
        run;

        proc append base=&out. data=work._fill_mon_num_long force;
        run;
    %end;

    %if %length(%superq(vars_cat)) > 0 %then %do;

        data work._fill_mon_cat_stage;
            set &data.(keep=Muestra &byvar. &vars_cat.);
            %let _cidx=1;
            %let _cvar=%scan(%superq(vars_cat), &_cidx., %str( ));
            %do %while(%length(%superq(_cvar)) > 0);
                _fill_cind_&_cidx.=(not missing(cats(&_cvar.)));
                %let _cidx=%eval(&_cidx. + 1);
                %let _cvar=%scan(%superq(vars_cat), &_cidx., %str( ));
            %end;
        run;

        proc summary data=work._fill_mon_cat_stage nway;
            class Muestra &byvar.;
            var _fill_cind_:;
            output out=work._fill_mon_cat(drop=_type_ _freq_) sum=;
        run;

        proc sort data=work._fill_mon_cat;
            by Muestra &byvar.;
        run;
        proc sort data=work._fill_mon_tot;
            by Muestra &byvar.;
        run;

        data work._fill_mon_cat_long;
            merge work._fill_mon_tot(in=_a) work._fill_mon_cat(in=_b);
            by Muestra &byvar.;
            if not (_a and _b) then delete;

            length Variable $64 Var_Type $8;
            Var_Type='CAT';

            %let _cidx=1;
            %let _cvar=%scan(%superq(vars_cat), &_cidx., %str( ));
            %do %while(%length(%superq(_cvar)) > 0);
                Variable=upcase("&_cvar.");
                N_Filled=_fill_cind_&_cidx.;
                Fillrate=(N_Filled / N_Total) * 100;
                output;
                %let _cidx=%eval(&_cidx. + 1);
                %let _cvar=%scan(%superq(vars_cat), &_cidx., %str( ));
            %end;

            keep Variable Var_Type Muestra &byvar. N_Total N_Filled Fillrate;
        run;

        proc append base=&out. data=work._fill_mon_cat_long force;
        run;
    %end;

    proc sort data=&out.;
        by Variable Muestra &byvar.;
    run;

    proc datasets library=work nolist nowarn;
        delete _fill_mon_tot _fill_mon_num _fill_mon_num_long
               _fill_mon_cat_stage _fill_mon_cat _fill_mon_cat_long;
    quit;

%mend _fill_monthly_compute;

%macro _fill_plot_monthly(data=, byvar=, image_stub=);

    %local _nvars _i _var;

    proc sql noprint;
        select count(distinct Variable) into :_nvars trimmed from &data.;
        select distinct Variable into :_fill_plot_v1- from &data.;
    quit;

    %if %length(%superq(_nvars))=0 %then %let _nvars=0;

    %do _i=1 %to &_nvars.;
        %let _var=&&_fill_plot_v&_i.;
        ods graphics / imagename="&image_stub._&_i." imagefmt=jpeg;
        title "Fillrate completo - &_var.";

        proc sgplot data=&data.(where=(Variable="&_var."));
            series x=&byvar. y=Fillrate /
                group=Muestra
                lineattrs=(thickness=2);
            xaxis type=discrete label="&byvar.";
            yaxis label="Fillrate (%)" min=0 max=100;
            keylegend / title="Muestra";
        run;

        title;
    %end;

%mend _fill_plot_monthly;
