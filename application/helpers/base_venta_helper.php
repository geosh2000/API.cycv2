<?php

class venta_help{

    public static function base($class, $inicio, $fin, $prod = false, $pais = null, $mp = true, $outlet = false, $ag = false){

      
        $class->db->select("a.*, canal, gpoCanal, gpoCanalKpi, marca, pais, tipoCanal, c.dep, vacante, puesto, cc, tipo")
            ->select("IF(CAST(dtCreated as DATE) = a.Fecha, Localizador, null) as NewLoc, CAST(dtCreated as DATE) as dtCreated", FALSE)
            ->select('ml.asesor')
            ->from('t_hoteles_test a')
            ->join("t_masterlocators ml", "a.Localizador = ml.masterlocatorid", "left")          
            ->join("chanGroups b", "a.chanId = b.id", "left")
            ->join("dep_asesores c", "ml.asesor = c.asesor AND a.Fecha = c.Fecha", "left")
            ->join("cc_apoyo d", "ml.asesor = d.asesor AND a.Fecha BETWEEN d.inicio AND d.fin", "left")
            ->where("a.Fecha BETWEEN", "'$inicio' AND '$fin'", FALSE);
    
        if( $mp ){
            $class->db->where( array( 'marca' => 'Marcas Propias' ) );
        }else{
            $class->db->where( array( 'marca' => 'Marcas Terceros' ) )
                    ->where( array( 'gpoCanalKpi !=' => 'Avianca') )
                    ->where( array( 'gpoCanalKpi !=' => 'COOMEVA') );
        }

        if( $pais != null ){ $class->db->where_in('pais', $pais);  }
        if( $pais == null && $mp ){ $class->db->where('pais !=', 'CO');  }
        if( !$outlet ){ $class->db->where( array( 'gpoCanalKPI !=' => 'Outlet' ) ); }
        if( !$ag ){ $class->db->where( array( 'gpoCanalKPI !=' => 'Agencias' ) ); }
            

        $tableLocs = $class->db->get_compiled_select();
        $class->db->query("DROP TEMPORARY TABLE IF EXISTS base");

        if( $class->db->query("CREATE TEMPORARY TABLE base $tableLocs") ){
            return $tableLocs;
        }else{
            errResponse('Error al compilar tabla base', REST_Controller::HTTP_BAD_REQUEST, $class, 'error', $class->db->error());
        }
    }

  public static function ventaF($class, $inicio, $fin, $type, $td=false, $skill, $params, $ag = false){

    $fecha = "a.Fecha BETWEEN";
    $fechaVar = "'$inicio' AND '$fin'";
    switch($params['sede']){
        case 'CO':
            $currency = 'COP';
            $margen = "costoCOP";
            break;
        case 'MX':
            $currency = 'MXN';
            $margen = "costo";
            break;
        default:
            $currency = 'MXN';
            $margen = "costo";
            break;
    }

    $class->db->query("DROP TEMPORARY TABLE IF EXISTS locsProdF");

    $class->db->select('a.*, ml.tipo')
            ->select("  CASE 
                        WHEN ml.asesor = 0 AND 3 != ".$params['skin']." THEN 0
                        WHEN tipoRsva LIKE '%Tag%' THEN 50 
                        WHEN tipoRsva LIKE '%CO_Hoteles%' THEN 73 
                        WHEN tipoRsva LIKE '%Out' THEN ".$params['skout']."
                        WHEN tipoRsva LIKE '%IN' THEN ".$params['skin']."
                        WHEN tipoRsva LIKE '%Presencial%' THEN 29 ELSE 0 END as Skill,
                    CASE
                        WHEN ml.asesor = 0 THEN 'Otros'
                        WHEN
                            tipoRsva LIKE '%PDV%'
                        THEN
                            CASE
                                WHEN cc IS NULL THEN 'PDV'
                                WHEN cc IS NOT NULL THEN 'Apoyo'
                            END
                        WHEN tipoRsva LIKE '%Presencial%' THEN 'Presencial'
                        WHEN tipoRsva LIKE '%online%' THEN 'Online'
                        WHEN
                            tipoRsva LIKE '%out%'
                        THEN
                            CASE
                                WHEN tipoRsva LIKE 'out' THEN 'CC'
                                WHEN tipoRsva LIKE '%Tag%' THEN 'CC'
                                ELSE 'Otros'
                            END
                        WHEN
                            tipoRsva LIKE '%IN'
                        THEN
                            CASE
                                WHEN cc IS NULL THEN 'CC'
                                WHEN cc IS NOT NULL THEN 'Apoyo'
                            END
                        ELSE 'CC'
                    END AS Grupo,
                    SUM(Venta$currency + OtrosIngresos$currency + Egresos$currency) AS monto,
                    SUM(IF(servicio = 'Hotel',
                        Venta$currency + OtrosIngresos$currency + Egresos$currency,
                        0)) AS monto_hotel,
                    SUM(IF(servicio = 'Tour',
                        Venta$currency + OtrosIngresos$currency + Egresos$currency,
                        0)) AS monto_tour,
                    SUM(IF(servicio = 'Transfer',
                        Venta$currency + OtrosIngresos$currency + Egresos$currency,
                        0)) AS monto_transfer,
                    SUM(IF(servicio = 'Vuelo',
                        Venta$currency + OtrosIngresos$currency + Egresos$currency,
                        0)) AS monto_vuelo,
                    SUM(IF(servicio = 'Seguro',
                        Venta$currency + OtrosIngresos$currency + Egresos$currency,
                        0)) AS monto_seguro,
                    SUM(clientNights) AS RNs,
                    SUM($margen) AS margen,
                    IF(tipoRsva LIKE '%OUT%'
                            AND IF(dtCreated BETWEEN a.Fecha AND ADDDATE(a.Fecha, 1),
                            a.Localizador,
                            NULL) IS NOT NULL
                            AND SUM(Venta$currency + OtrosIngresos$currency + Egresos$currency) > 0,
                        a.Localizador,
                        NULL) AS CountLocOut,
                    IF(tipoRsva LIKE '%IN%'
                            AND IF(dtCreated BETWEEN a.Fecha AND ADDDATE(a.Fecha, 1),
                            a.Localizador,
                            NULL) IS NOT NULL
                            AND SUM(Venta$currency + OtrosIngresos$currency + Egresos$currency) > 0,
                        a.Localizador,
                        NULL) AS CountLocIn, servicio", FALSE)
            ->from("t_hoteles_test a")
            ->join("t_masterlocators ml", "a.Localizador = ml.masterlocatorid", 'left')
            ->join("chanGroups b", "a.chanId = b.id", 'left')
            ->join("dep_asesores dp", "ml.asesor = dp.asesor AND a.Fecha = dp.Fecha", 'left', FALSE)
            ->join("cc_apoyo ap", "ml.asesor = ap.asesor AND a.Fecha BETWEEN ap.inicio AND ap.fin", 'left', FALSE)
            ->join("itemTypes c", "itemType = c.type AND categoryId = c.category", 'left',FALSE)
            ->join("t_margen d", "a.Localizador = d.Localizador AND a.item = d.Item AND a.Fecha = d.Fecha AND a.Hora=d.Hora", 'left', FALSE)
            ->join("config_tipoRsva tp", "IF(dp.dep IS NULL,
                    IF(ml.asesor = - 1, - 1, 0),
                    IF(dp.dep = 29 AND cc IS NOT NULL,
                            35,
                            dp.dep)) = tp.dep
                    AND IF(ml.tipo IS NULL OR ml.tipo = '',
                    0,
                    ml.tipo) = tp.tipo", 'left', FALSE)
            ->where($fecha, $fechaVar, FALSE)
            ->group_by(array('Fecha','Localizador', 'item'));

    if( $params['mp'] ){
        $class->db->where( array( 'marca' => 'Marcas Propias', 'pais' => $params['sede'] ) );
    }else{
        if( $params['sede'] == 'MX' ){
            $class->db->where( array( 'marca' => 'Marcas Terceros', 'gpoCanalKpi !=' => 'Avianca') )
                    ->where( array( 'gpoCanalKpi !=' => 'Outlet') );
                }
        if( $params['isAgency'] == 0 ){$class->db->where( array( 'gpoCanalKpi !=' => 'Agencias') );}
    }

    $tableLocs = $class->db->get_compiled_select();

    IF($class->db->query("CREATE TEMPORARY TABLE locsProdF $tableLocs")){    
   
    return $tableLocs;
    }else{
    errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $class, 'error', $class->db->error());
    }


  }

  

}

?>


 
