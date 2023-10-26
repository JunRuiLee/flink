/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { NgForOf, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { Subject } from 'rxjs';
import { mergeMap, takeUntil } from 'rxjs/operators';

import { JobConfig } from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzTableModule } from 'ng-zorro-antd/table';

import { JobLocalService } from '../job-local.service';

@Component({
  selector: 'flink-job-configuration',
  templateUrl: './job-configuration.component.html',
  styleUrls: ['./job-configuration.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzCardModule, NzTableModule, NgIf, NgForOf, NzCollapseModule],
  standalone: true
})
export class JobConfigurationComponent implements OnInit, OnDestroy {
  public config: JobConfig;
  public listOfUserConfig: Array<{ key: string; value: string }> = [];
  public listOfJobConfig: Array<{ key: string; value: string }> = [];

  private destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.jobLocalService
      .jobDetailChanges()
      .pipe(
        mergeMap(job => this.jobService.loadJobConfig(job.jid)),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.config = data;
        const userConfig = this.config['execution-config']['user-config'];
        const userConfigArray = [];
        for (const key in userConfig) {
          userConfigArray.push({
            key,
            value: userConfig[key]
          });
        }
        this.listOfUserConfig = userConfigArray.sort((pre, next) => (pre.key > next.key ? 1 : -1));
        const jobConfig2 = this.config['job-config'];
        const jobConfigArray = [];
        for (const key in jobConfig2) {
          jobConfigArray.push({
            key,
            value: jobConfig2[key]
          });
        }
        this.listOfJobConfig = jobConfigArray.sort((pre, next) => (pre.key > next.key ? 1 : -1));
        this.cdr.markForCheck();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
