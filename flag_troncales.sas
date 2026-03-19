libname maindata "&_root_path/Data";
libname tcl_&i "&_root_path/Troncal_&i/Data";


%if %length(&flag_tcl) = 0 %then %do;
      %put dentro de sin flag;
      data tcl_&i..&&train_tcl_&i;
      set maindata.&master_table;
      where &&byvar_&i >= &&train_min_mes_&i and &&byvar_&i <= &&train_max_mes_&i;
      run;
      data tcl_&i..&&oot_tcl_&i;
      set maindata.&master_table;
      where &&byvar_&i >= &&oot_min_mes_&i and &&byvar_&i <= &&oot_max_mes_&i;
      run;
%end;
%else %do;
      %let flag_col = %scan(&flag_tcl, &i);
      
      data tcl_&i..&&train_tcl_&i;
      set maindata.&master_table;
      where &flag_col = 1 and &&byvar_&i >= &&train_min_mes_&i and &&byvar_&i <= &&train_max_mes_&i;
      run;
      
      data tcl_&i..&&oot_tcl_&i;
      set maindata.&master_table;
      where &flag_col = 1 and &&byvar_&i >= &&oot_min_mes_&i and &&byvar_&i <= &&oot_max_mes_&i;
      run;
%end;