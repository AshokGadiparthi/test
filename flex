<ng-container matColumnDef="status">
  <th mat-header-cell *matHeaderCellDef> Status </th>
  <td mat-cell *matCellDef="let element" class="flex justify-center items-center">
    <span
      [style.width.px]="16"
      [style.height.px]="16"
      style="border-radius:50%; display:inline-block;"
      [style.background-color]="
        element.status === 'red' ? '#f87171' :
        element.status === 'yellow' ? '#facc15' :
        '#34d399'
      ">
    </span>
  </td>
</ng-container>
