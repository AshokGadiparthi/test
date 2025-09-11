<ng-container matColumnDef="status">
  <th mat-header-cell *matHeaderCellDef> Status </th>
  <td mat-cell *matCellDef="let element" class="flex justify-center">
    <span
      style="width:16px; height:16px; border-radius:50%; display:inline-block; background-color:
        {{ element.status === 'red' ? '#f87171' : element.status === 'yellow' ? '#facc15' : '#34d399' }}">
    </span>
  </td>
</ng-container>
