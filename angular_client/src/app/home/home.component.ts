import { Component, OnInit, OnDestroy } from '@angular/core';
import { ChartDataSets, ChartOptions } from 'chart.js';
import { Color, Label } from 'ng2-charts';
import { DataService } from '@app/_services/data.service';

@Component({ templateUrl: 'home.component.html' })
export class HomeComponent implements OnInit {
    public lineChartData: ChartDataSets[];
    //  = [
    //     { data: [65, 59, 80, 81, 56, 55, 40], label: 'Series A' },
    // ];
    public lineChartLabels: Label[] = ['January', 'February', 'March', 'April', 'May', 'June', 'July'];
    public lineChartOptions = {
        responsive: true,
    };
    public lineChartColors: Color[] = [
        {
            borderColor: 'black',
            backgroundColor: '#80b9ff',
        },
    ];
    public lineChartLegend = true;
    public lineChartType = 'line';
    public lineChartPlugins = [];

    constructor(private dataService: DataService) { }

    ngOnInit() {
        this.dataService.getAllData().subscribe(dataArr => {
            this.lineChartData = [{ data: dataArr.map(data => data.count), label: 'Road Crash' }];
            this.lineChartLabels = dataArr.map(data => data.month + ' ' + data.year)
        });
    }
}