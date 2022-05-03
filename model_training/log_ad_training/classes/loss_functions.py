import torch


class NuLogsyLossCompute:
    """A simple loss compute and train function."""

    def __init__(self, model, criterion_logsy, criterion_nulog, opt=None, ):
        self.model = model
        self.criterion_logsy = criterion_logsy
        self.criterion_nulog = criterion_nulog
        self.opt = opt

    def __call__(self, out, y, y_mask, norm=1.0, is_test=False):

        # logsy
        dist = torch.sum((out[:, 0, :] - self.model.c) ** 2, dim=1)
        loss_l = torch.mean((1 - y) * torch.sqrt(dist) - y * torch.log(1 - torch.exp(-torch.sqrt(dist))))

        # nulog
        x = self.model.generator(out)
        y_mask = y_mask.reshape(-1)
        loss_nu = self.criterion_nulog(x, y_mask) * norm
        loss = loss_l + (loss_nu / 5)
        # loss = loss_l
        if not is_test:
            loss.backward()
            if self.opt is not None:
                self.opt.step()
                self.opt.zero_grad()

        return loss.item(), loss_nu ,loss_l
        # return loss.item() * norm
