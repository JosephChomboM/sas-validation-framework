/* =========================================================================
universe_compute.sas - CAS-first descriptive computations for Universe

The module operates on a single CAS table that already contains TRAIN and OOT
rows together. Differences are exposed only through a split column.
======================================================================== */
%macro _univ_sort_cas(table_name=, orderby=, groupby={});

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

%mend _univ_sort_cas;

%macro _univ_describe_id(data=, split_var=_univ_split, byvar=, id_var=);

    title "Evolutivo Cuentas";

    proc fedsql sessref=conn;
        create table casuser._univ_evolut_cuenta {options replace=true} as
        select &split_var., &byvar., count(*) as Count
        from &data.
        group by &split_var., &byvar.;
    quit;

    %_univ_sort_cas(table_name=_univ_evolut_cuenta,
        orderby=%str({"&split_var.", "&byvar."}));

    proc print data=casuser._univ_evolut_cuenta noobs;
    run;

    proc sgplot data=casuser._univ_evolut_cuenta;
        vbarparm category=&byvar. response=Count /
            group=&split_var.
            groupdisplay=cluster
            nooutline;
        yaxis label="Cuentas" min=0;
        xaxis label="&byvar." type=discrete;
    run;

    proc fedsql sessref=conn;
        create table casuser._univ_dup {options replace=true} as
        select &split_var., &byvar., &id_var., count(*) as N
        from &data.
        group by &split_var., &byvar., &id_var.
        having count(*) > 1;
    quit;

    %_univ_sort_cas(table_name=_univ_dup,
        orderby=%str({"&split_var.", "&byvar.", "&id_var."}));

    title "Duplicados por periodo";
    proc print data=casuser._univ_dup noobs;
    run;

    title;

    proc datasets library=casuser nolist nowarn;
        delete _univ_evolut_cuenta _univ_dup;
    quit;

%mend _univ_describe_id;

%macro _univ_bandas_cuentas(data=, split_var=_univ_split, byvar=, id_var=);

    %global _univ_mean _univ_std;
    %local inf sup max_val _univ_max_count;

    proc fedsql sessref=conn;
        create table casuser._univ_sindup {options replace=true} as
        select distinct &split_var., &byvar., &id_var.
        from &data.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._univ_freq_cuentas {options replace=true} as
        select &split_var., &byvar., count(*) as Count
        from casuser._univ_sindup
        group by &split_var., &byvar.;
    quit;

    %_univ_sort_cas(table_name=_univ_freq_cuentas,
        orderby=%str({"&split_var.", "&byvar."}));

    proc sql noprint;
        select coalesce(mean(Count), 0),
               coalesce(std(Count), 0)
          into :_univ_mean trimmed,
               :_univ_std trimmed
        from casuser._univ_freq_cuentas
        where upcase(&split_var.)='TRAIN';

        select coalesce(max(Count), 0)
          into :_univ_max_count trimmed
        from casuser._univ_freq_cuentas;
    quit;

    %if %sysevalf(%superq(_univ_mean)=, boolean) %then %let _univ_mean=0;
    %if %sysevalf(%superq(_univ_std)=, boolean) %then %let _univ_std=0;
    %if %sysevalf(%superq(_univ_max_count)=, boolean) %then
        %let _univ_max_count=0;

    %let inf=%sysevalf(&_univ_mean. - 2 * &_univ_std.);
    %if %sysevalf(&inf. < 0) %then %let inf=0;
    %let sup=%sysevalf(&_univ_mean. + 2 * &_univ_std.);
    %let max_val=%sysevalf(&_univ_max_count. * 1.05);
    %if %sysevalf(&sup. > &max_val.) %then
        %let max_val=%sysevalf(&sup. * 1.05);

    proc fedsql sessref=conn;
        create table casuser._univ_freq_cuentas_plot {options replace=true} as
        select *,
               &inf. as LowerBand,
               &sup. as UpperBand
        from casuser._univ_freq_cuentas;
    quit;

    title "Evolutivo Cuentas (+/-2 sigma TRAIN)";
    proc print data=casuser._univ_freq_cuentas noobs;
    run;

    proc sgplot data=casuser._univ_freq_cuentas_plot subpixel;
        band x=&byvar. lower=LowerBand upper=UpperBand /
            fillattrs=(color=graydd)
            transparency=0.5
            legendlabel="Banda TRAIN +/- 2 Desv. Estandar"
            name="band1";
        series x=&byvar. y=Count /
            group=&split_var.
            markers
            lineattrs=(thickness=2)
            name="serie1";
        refline &_univ_mean. /
            lineattrs=(color=red pattern=Dash)
            legendlabel="Mean TRAIN"
            name="line1";
        yaxis min=0 max=&max_val. label="Cuentas";
        xaxis label="&byvar." type=discrete;
        keylegend "serie1" "band1" "line1" /
            location=inside position=bottomright;
    run;

    title;

    proc datasets library=casuser nolist nowarn;
        delete _univ_sindup _univ_freq_cuentas _univ_freq_cuentas_plot;
    quit;

%mend _univ_bandas_cuentas;

%macro _univ_evolutivo_monto(data=, split_var=_univ_split, monto_var=,
    byvar=);

    proc fedsql sessref=conn;
        create table casuser._univ_sum_monto {options replace=true} as
        select &split_var., &byvar., sum(&monto_var.) as Sum_Monto
        from &data.
        group by &split_var., &byvar.;
    quit;

    %_univ_sort_cas(table_name=_univ_sum_monto,
        orderby=%str({"&split_var.", "&byvar."}));

    title "Suma &monto_var. por &byvar.";

    proc print data=casuser._univ_sum_monto noobs;
    run;

    proc sgplot data=casuser._univ_sum_monto;
        vbarparm category=&byvar. response=Sum_Monto /
            group=&split_var.
            groupdisplay=cluster;
        xaxis label="&byvar." type=discrete;
        yaxis label="&monto_var.";
    run;

    title;

    proc datasets library=casuser nolist nowarn;
        delete _univ_sum_monto;
    quit;

%mend _univ_evolutivo_monto;

%macro _univ_describe_monto(data=, split_var=_univ_split, monto_var=,
    byvar=);

    proc fedsql sessref=conn;
        create table casuser._univ_evolut_monto {options replace=true} as
        select &split_var.,
               &byvar.,
               count(&monto_var.) as N,
               avg(&monto_var.) as Mean
        from &data.
        group by &split_var., &byvar.;
    quit;

    %_univ_sort_cas(table_name=_univ_evolut_monto,
        orderby=%str({"&split_var.", "&byvar."}));

    title "Evolutivo &monto_var.";

    proc print data=casuser._univ_evolut_monto noobs;
    run;

    proc sgplot data=casuser._univ_evolut_monto;
        series x=&byvar. y=Mean /
            group=&split_var.
            markers
            lineattrs=(thickness=2);
        yaxis label="mean &monto_var." valuesformat=COMMA16.0 min=0;
        xaxis label="&byvar." type=discrete;
    run;

    title;

    proc datasets library=casuser nolist nowarn;
        delete _univ_evolut_monto;
    quit;

%mend _univ_describe_monto;
