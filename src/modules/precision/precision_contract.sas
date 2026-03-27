/* =========================================================================
precision_contract.sas - Validaciones pre-ejecucion del modulo Precision
========================================================================= */
%macro precision_contract(input_caslib=, input_table=, target=, score_var=,
    byvar=, def_cld=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=, monto_var=, segvar=);

    %let _prec_rc=0;

    %local _prec_nobs_scope _prec_nobs_trn _prec_nobs_oot _prec_has_col;

    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [precision_contract] target no definido.;
        %let _prec_rc=1;
        %return;
    %end;

    %if %length(%superq(score_var))=0 %then %do;
        %put ERROR: [precision_contract] score_var no definido.;
        %let _prec_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [precision_contract] byvar no definido.;
        %let _prec_rc=1;
        %return;
    %end;

    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [precision_contract] def_cld no definido.;
        %let _prec_rc=1;
        %return;
    %end;

    %if %length(%superq(train_min_mes))=0 or %length(%superq(train_max_mes))=0
        or %length(%superq(oot_min_mes))=0 or %length(%superq(oot_max_mes))=0
        %then %do;
        %put ERROR: [precision_contract] Ventanas TRAIN/OOT no resueltas.;
        %let _prec_rc=1;
        %return;
    %end;

    proc sql noprint;
        select count(*) into :_prec_nobs_scope trimmed
        from &input_caslib..&input_table.;
        select count(*) into :_prec_nobs_trn trimmed
        from &input_caslib..&input_table.
        where &byvar. >= &train_min_mes.
          and &byvar. <= &train_max_mes.;
        select count(*) into :_prec_nobs_oot trimmed
        from &input_caslib..&input_table.
        where &byvar. >= &oot_min_mes.
          and &byvar. <= &oot_max_mes.;
    quit;

    %if &_prec_nobs_scope.=0 %then %do;
        %put ERROR: [precision_contract] input vacio o no accesible.;
        %let _prec_rc=1;
        %return;
    %end;

    %if &_prec_nobs_trn.=0 %then %do;
        %put ERROR: [precision_contract] TRAIN derivado vacio en ventana
            &train_min_mes.-&train_max_mes..;
        %let _prec_rc=1;
        %return;
    %end;

    %if &_prec_nobs_oot.=0 %then %do;
        %put ERROR: [precision_contract] OOT derivado vacio en ventana
            &oot_min_mes.-&oot_max_mes..;
        %let _prec_rc=1;
        %return;
    %end;

    %macro _prec_chk(mem=, col=, req=1);
        %let _prec_has_col=0;
        proc sql noprint;
            select count(*) into :_prec_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&mem.")
              and upcase(name)=upcase("&col.");
        quit;
        %if &_prec_has_col.=0 %then %do;
            %if &req.=1 %then %do;
                %put ERROR: [precision_contract] &col. no encontrada en &mem..;
                %let _prec_rc=1;
            %end;
            %else %do;
                %put WARNING: [precision_contract] &col. no encontrada en &mem..;
            %end;
        %end;
    %mend;

    %_prec_chk(mem=&input_table., col=&target., req=1);
    %if &_prec_rc.=1 %then %return;

    %_prec_chk(mem=&input_table., col=&score_var., req=1);
    %if &_prec_rc.=1 %then %return;

    %_prec_chk(mem=&input_table., col=&byvar., req=1);
    %if &_prec_rc.=1 %then %return;

    %if %length(%superq(monto_var)) > 0 %then
        %_prec_chk(mem=&input_table., col=&monto_var., req=0);
    %if %length(%superq(segvar)) > 0 %then
        %_prec_chk(mem=&input_table., col=&segvar., req=0);

    %put NOTE: [precision_contract] OK - input=&input_caslib..&input_table.;
    %put NOTE: [precision_contract] target=&target. score=&score_var.
        byvar=&byvar. def_cld=&def_cld. monto=&monto_var. segvar=&segvar.;
    %put NOTE: [precision_contract] ventanas TRAIN=&train_min_mes.-&train_max_mes.
        OOT=&oot_min_mes.-&oot_max_mes..;

%mend precision_contract;
