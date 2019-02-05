importScripts("d3.js")



onmessage = function(msg) {

    var width = msg.data.width,
        height = msg.data.height,
        json = msg.data.json,
        rid = msg.data.rid;


    // Carga de datos/json
    // d3.json(json, function(error, graph) {

        var max = d3.max(json.nodes, function(d) { return d.degree_origin; });
        var min = d3.min(json.nodes, function(d) { return d.degree_origin; });


    
        var radius = d3.scaleSqrt()
            .domain([min,max+100])
            .range([6, 12]);
    
        function degreeF(node){
            if(node.class == "guest"){
                return max+100
            }
            else if(node.class == "relevant"){
                return max+100
            }
            else return node.degree_origin
        }


        // In theory is the fastest way to find a max value in javascript...
        function findMaxLevel(arr) {
            let max = Math.max.apply(Math, arr[0].levels);
          
            for (let i = 1, len=arr.length; i < len; i++) {
              let v = Math.max.apply(Math, arr[i].levels);
              max = (v > max) ? v : max;
            }
          
            return max+1;
        }

        var maxLevel = findMaxLevel(json.nodes);

        //.distance(function(d) { return radius(d.source.degree*10 / 2) + radius(d.target.degree*10 / 2); })
        //d3.forceCollide().radius(function(d) { return radius(degreeF(d) * 6); })
        var simulation = d3.forceSimulation()
            .force("link", 
                d3.forceLink().id(function(d) { return d.index; })
                .strength(function(d) {return 0.15; }).iterations(50)
                )
            .force("charge", d3.forceManyBody().strength(-3000))
            .force("center", d3.forceCenter(width / 2, height / 2))
            .force("collide", d3.forceCollide().radius(function(d) { return radius(degreeF(d) * 6) + 5; }))
            .stop();

        simulation
            .nodes(json.nodes);
            //.on("tick", ticked);

        simulation.force("link")
            .links(json.links);

        simulation.alpha(1).restart();

        // See https://github.com/d3/d3-force/blob/master/README.md#simulation_tick
        for (var i = 0, n = Math.ceil(Math.log(simulation.alphaMin()) / Math.log(1 - simulation.alphaDecay())); i < n; ++i) {
            simulation.tick();
            
            //Contamos la cantidad de nodos "guest"    
            var count = 0;
            for(var j = 0; j < json.nodes.length; ++j){
                if(json.nodes[j].class == "guest")
                    count++;
            }

            //====================================================================================
            //function returns small and big radiuses of annulus based on Point year
            function getAnnulus(levels){
                //var big_radius;
                var separator = 150;


                if(levels.length > 1){

                    var max_level = Math.max.apply(Math, levels);
                    var min_level = Math.min.apply(Math, levels);

                    var big_radius = (max_level + 1) * separator;
                    var small_radius = (min_level + 1) * separator;

                    //return [big_radius - separator, big_radius];
                    return [small_radius, big_radius];
                }else{
                    var big_radius = (levels[0] + 1) * separator;
                    return [big_radius - separator/2, big_radius]
                }
            }

            //function to verify if X in the correct position
            function verifyPosition(x, y, small_r,big_r){
                var point;

                var cx = width/2;
                var cy = height/2;
                //verify if P is in annulus defined by small_r and big_r
                if ( (Math.pow(x - cx,2) + Math.pow(y - cy, 2)) <= Math.pow(small_r,2) ){
                // P inside small circle
                point = recalculateP(x, y, small_r);
                } else if ( (Math.pow(x - cx, 2) + Math.pow(y - cy, 2)) > Math.pow(big_r,2)){
                // P outside big circle
                point = recalculateP(x, y, big_r);
                } else {
                point = [x,y];
                }
                return point;
            }

            //places point off circle on circle ring
            function recalculateP(x, y, r){
                var cx = width/2;
                var cy = height/2;

                var vx = x - cx;
                var vy = y - cy;
                var norm = Math.sqrt(Math.pow(vx,2)+ Math.pow(vy,2));
                var new_x = cx + vx / norm * r;
                var new_y = cy + vy / norm * r;
                return [new_x,new_y];
            }
            //====================================================================================

            // evenly spaces nodes along arc
            function circleCoord(i, numNodes, r){
                angle = (i / (numNodes/2)) * Math.PI; // Calculate the angle at which the element will be placed.
                // For a semicircle, we would use (i / numNodes) * Math.PI.
                x = (r * Math.cos(angle)) + (width/2); // Calculate the x position of the element.
                y = (r * Math.sin(angle)) + (height/2); // Calculate the y position of the element.

                return [x, y];
            };

            var c0 = 0;
            json.nodes.forEach(function(n) {
                /*var annulus = getAnnulus(n.levels);
                var position = verifyPosition(n.x, n.y, annulus[0], annulus[1]);
                n.x = position[0];
                n.y = position[1];*/
                if (n.class == "guest"){
                    var annulus = getAnnulus(n.levels);
                    var position = circleCoord(c0, count, annulus[1]);
                    c0++
                    n.x = position[0];
                    n.y = position[1];
                }else if(n.levels.includes(0)){
                    n.x = width/2;
                    n.y = height/2;
                }
                else{
                    var annulus = getAnnulus(n.levels);
                    var position = verifyPosition(n.x, n.y, annulus[0], annulus[1]);
                    n.x = position[0];
                    n.y = position[1];
                }
            });
        }

        postMessage(json);

    //});
};