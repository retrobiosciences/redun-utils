"""src/redun-utils/tasks.py"""
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from redun import task
from redun.scheduler import catch, catch_all, throw


@dataclass(named=True)
class TaskResourcePolicy:
    vcpus: int
    memory: float
    boot_disk_size_gib: float
    machine_type: Optional[str] = None
    accelerators: Optional[Tuple[str, int]] = None
    valid_cpu_memory_pairs = [(8, 32), (8, 64), (16, 128), (32, 256)]

    def task_options(self) -> Dict[str, Any]:
        result = dict(
            vcpus=self.vcpus,
            memory=self.memory,
            boot_disk_size_gib=self.boot_disk_size_gib,
        )
        if self.machine_type is not None:
            result["machine_type"] = self.machine_type
        if self.accelerators is not None:
            result["accelerators"] = self.accelerators
        return result

    def attempt_options(self, attempt: int) -> Dict[str, Any]:
        if self.machine_type is not None:
            # double last digit of machine type
            machine_type_name = self.machine_type.rsplit("-", 1)[0]
            machine_type_num_identifier = int(self.machine_type.rsplit("-", 1)[1])
            new_machine_type = f"{machine_type_name}-{machine_type_num_identifier * 2**(attempt-1)}"
            result = {
                "machine_type": new_machine_type,
                "vcpus": self.vcpus * 2 ** (attempt - 1),
                "memory": self.memory * 2 ** (attempt - 1),
            }
            if self.accelerators is not None:
                result["accelerators"] = self.accelerators
            return result

        # find the first pair that fulfills the requirement
        cpu, memory = next(
            (cpu, memory) for cpu, memory in self.valid_cpu_memory_pairs if cpu >= self.vcpus and memory >= self.memory
        )

        # if attempt is greater than 1, we shift the selected pair by attempt - 1 positions
        if attempt > 1:
            index = self.valid_cpu_memory_pairs.index((cpu, memory)) + attempt - 1
            cpu, memory = (
                self.valid_cpu_memory_pairs[index] if index < len(self.valid_cpu_memory_pairs) else (None, None)
            )
        result = {
            "vcpus": cpu,
            "memory": memory,
            "boot_disk_size_gib": self.boot_disk_size_gib,
        }
        if self.accelerators is not None:
            result["accelerators"] = self.accelerators
        return result


@task
def retry_on_fail(task_obj, option_updater: Callable[[int], Dict[str, Any]], attempt=1, retries=3, **kwargs):
    task_options = option_updater(attempt)
    if attempt == retries:
        return task_obj.options(**task_options)(**kwargs)

    @task
    def retry_on_fail_inner(error, task_obj, option_updater: Callable, attempt: int, retries, **kwargs):
        print(f"Task failed with:\n{error}, retrying...")
        return retry_on_fail(task_obj, option_updater, attempt + 1, retries, **kwargs)

    return catch(
        task_obj.options(**task_options)(**kwargs),
        Exception,
        retry_on_fail_inner.partial(
            task_obj=task_obj, option_updater=option_updater, attempt=attempt, retries=retries, **kwargs
        ),
    )


@task(cache=False)
def lazy_throw(error):
    print(f"Task failed with:\n{error}, throwing...")
    return throw(error)


def eval_or_defer_error(task_obj):
    """
    Returns the task's result if it succeeds, otherwise defers the error to the parent task.
    """
    return catch(task_obj, Exception, lazy_throw)


@task(cache=False)
def aggregate_errors(values: List[Any]):
    nerrors = sum(1 for value in values if isinstance(value, Exception))
    exceptions = [value for value in values if isinstance(value, Exception)]
    exceptions_str = "--------------------\n\n---------------------".join([str(e) for e in exceptions])
    raise Exception(f"{nerrors} error(s) occurred.\n\n{exceptions_str}")

def eval_all_or_aggregate_errors(task_objs: List[Any]):
    """
    Returns the results of all tasks if they succeed, otherwise aggregates the errors.
    """
    return catch_all(task_objs, Exception, aggregate_errors)
