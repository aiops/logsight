
var gauge_config = {
  type: 'doughnut',
  data: {
    datasets: [{
      label: '# of Votes',
      data: [70, 100],
      backgroundColor: ["Green", "Gray"]
    }]
  },
  options: {
    responsive: false,
    maintainAspectRatio: true,
    rotation: 270, // start angle in degrees
    circumference: 180, // sweep angle in degrees
  }
}

var gauge_config_old = {
  type: 'gauge',
  data: {
    datasets: [{
      data: [60, 70, 90, 100],
      value: 76,
      backgroundColor: ['green', 'yellow', 'orange', 'red'],
      borderWidth: 2
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: true,
    title: {
      display: false,
      text: 'Gauge chart'
    },
    layout: {
      padding: {
        bottom: 0
      }
    },
    needle: {
      // Needle circle radius as the percentage of the chart area width
      radiusPercentage: 2,
      // Needle width as the percentage of the chart area width
      widthPercentage: 3.2,
      // Needle length as the percentage of the interval between inner radius (0%) and outer radius (100%) of the arc
      lengthPercentage: 80,
      // The color of the needle
      color: 'rgba(0, 0, 0, 1)'
    },
    valueLabel: {
      formatter: Math.round
    }
  }
};

//window.onload = function() {
//  var ctx = document.getElementById('chart').getContext('2d');
//  window.myGauge = new Chart(ctx, gauge_config);
//};
//

