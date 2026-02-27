/* =========================================================================
   steps/03_select_troncal_segment.sas — Step 3: Config troncal/segmento (FRONTEND)

   La configuración de troncales y segmentos se define en config.sas
   porque requiere DATA steps para generar las tablas CAS:
     - casuser.cfg_troncales  (1 fila por troncal)
     - casuser.cfg_segmentos  (overrides por segmento)

   Este step documenta el contrato de UI: qué campos configura el
   usuario para cada troncal y segmento.

   >>> Para modificar troncales/segmentos, editar config.sas <<<

   Ref: design.md §4 (contrato de configuración), §5.1
   ========================================================================= */

/* ---- CONTRATO DE UI (_id_* variables) --------------------------------- */
/*                                                                          */
/*  Por troncal (en config.sas → casuser.cfg_troncales):                   */
/*                                                                          */
/*  _id_troncal_id         : ID numérico de la troncal (1, 2, ...)         */
/*  _id_id_var_id          : Variable identificador del cliente ($64)       */
/*  _id_target             : Variable target (binaria)                      */
/*  _id_pd                 : Variable probabilidad de default              */
/*  _id_xb                 : Variable score                                 */
/*  _id_monto              : Variable de monto                              */
/*  _id_byvar              : Variable temporal (YYYYMM)                     */
/*  _id_train_min_mes      : Mes mínimo ventana train (ej. 202301)         */
/*  _id_train_max_mes      : Mes máximo ventana train (ej. 202310)         */
/*  _id_oot_min_mes        : Mes mínimo ventana oot   (ej. 202311)         */
/*  _id_oot_max_mes        : Mes máximo ventana oot   (ej. 202401)         */
/*  _id_def_cld            : Periodo definición clásica (ej. 202401)       */
/*  _id_num_rounds         : Número de rondas de bootstrap                  */
/*  _id_threshold          : Umbral de decisión                             */
/*  _id_num_unv            : Lista vars numéricas universo (espacio-sep)    */
/*  _id_cat_unv            : Lista vars categóricas universo               */
/*  _id_dri_num_unv        : Lista vars numéricas drivers                   */
/*  _id_dri_cat_unv        : Lista vars categóricas drivers                */
/*  _id_var_seg            : Variable segmentadora ($64, vacío = sin seg)   */
/*  _id_n_segments         : Cantidad de segmentos numéricos (0 = sin seg) */
/*                                                                          */
/*  Por segmento (en config.sas → casuser.cfg_segmentos):                  */
/*                                                                          */
/*  _id_seg_num_list       : Override lista numéricas del segmento         */
/*  _id_seg_cat_list       : Override lista categóricas del segmento       */
/*  _id_seg_dri_num_list   : Override drivers numéricas del segmento       */
/*  _id_seg_dri_cat_list   : Override drivers categóricas del segmento     */
/*                            (vacío = hereda del troncal)                  */
/*                                                                          */
/* ----------------------------------------------------------------------- */

%put NOTE: ======================================================;
%put NOTE: [step-03] Configuración de troncal/segmento.;
%put NOTE:   La definición se encuentra en config.sas;
%put NOTE:   (casuser.cfg_troncales y casuser.cfg_segmentos).;
%put NOTE:   Ver design.md §4 para el contrato completo.;
%put NOTE: ======================================================;
