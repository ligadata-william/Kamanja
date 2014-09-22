/**
 * This method gets the SVG of the chart and uses canvg to draw it on a HTML5 canvas.
 * The canvas is then streamed to the browser window as a download.
 */
(function(H) {

    H.Chart.prototype.createCanvas = function(divId) {
        var svg = this.getSVG(),
            width = parseInt(svg.match(/width="([0-9]+)"/)[1]),
            height = parseInt(svg.match(/height="([0-9]+)"/)[1]),
            canvas = document.createElement('canvas');

        canvas.setAttribute('width', width);
        canvas.setAttribute('height', height);

        if (canvas.getContext && canvas.getContext('2d')) {

            canvg(canvas, svg);

            var image = canvas.toDataURL("image/png")
                .replace("image/png", "image/octet-stream");

            // Save locally
            window.location.href = image;
        } else {
            alert("Your browser doesn't support this feature, please use a modern browser");
        }

    };
}(Highcharts));