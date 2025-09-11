<ng-container matColumnDef="status">
  <th mat-header-cell *matHeaderCellDef> Status </th>
  <td mat-cell *matCellDef="let element" class="flex justify-center">
    <span
      class="w-4 h-4 rounded-full"
      [ngClass]="{
        'bg-red-500': element.status === 'red',
        'bg-yellow-400': element.status === 'yellow',
        'bg-green-500': element.status === 'green'
      }">
    </span>
  </td>
</ng-container>
